from prefect import flow, task, get_run_logger
from pathlib import Path
from tiled.client import from_profile

import event_model
import tqdm

tiled_client = from_profile("nsls2")["smi"]
tiled_client_raw = tiled_client["raw"]


def do_symlinking(
    links: list[tuple[str, Path, Path]],
    overwrite_dest=False,
) -> tuple[list[tuple[str, Path, Path]], list[tuple[str, Path, Path]]]:
    """
    Create the symlinks, making target directories as needed.

    Paramaters
    ----------
    links : list of (uid, src, dest) tuples
        The uid, source file and destination files

    overwrite_dest : bool, optional
        If an existing destitation should be unlinked and replaced.

    Returns
    -------
    linked, failed : list of (uid, src, dest) tuples
        The linked (or failed) values.
    """

    failed = []
    linked = []

    for uid, src, dest, analysis in tqdm.tqdm(links, leave=False):
        if not src.exists():
            failed.append((uid, src, dest, analysis))
            continue

        try:
            dest.parent.mkdir(exist_ok=True, parents=True)
            analysis.mkdir(exist_ok=True, parents=True)

            if overwrite_dest and dest.exists():
                dest.unlink()
            dest.symlink_to(src)

        except Exception:
            tqdm.tqdm.write(f"FAILED: {dest}")
            failed.append((uid, src, dest, analysis))
        else:
            tqdm.tqdm.write(f"Linked: {dest}")
            linked.append((uid, src, dest, analysis))
    return linked, failed


@task
def get_symlink_pairs(ref, *, det_map, root_map=None):
    """
    Parameters
    ----------
    ref : Union[int, str]
        Scan_id or uid of the start document
    det_map : dict[str, str]
        A dictionaly mapping the detector name (1M, 900KW)
        to the type of measurement (SAXS, WAXS)
    root_map : dict[str, str], optional
        A mapping of root in the resource document -> a new path
        as in databroker

    Returns
    -------
    list[tuple[str, Path, Path]]
         A tuple of the start uid, the source path and the destination path.
    """
    logger = get_run_logger()
    ########################
    if root_map is None:
        root_map = {}

    links = []
    target_template: str
    output_path: str
    resource_info = {}
    datum_info = {}
    target_keys = set()
    ########################

    # hrf = db[ref]
    hrf = tiled_client_raw[ref]
    for name, doc in hrf.documents():
        print(name)
        if name == "start":
            start_uid = doc["uid"]

            output_path = f"{doc['project_name']}/"
            target_template = f'{output_path}/{{det_name}}/{doc["username"]}_\
            {doc["sample_name"]}_id{doc["scan_id"]}_{{N:06d}}_{{det_type}}.tif'

            target_path = Path(
                f"/nsls2/data/smi/proposals/{doc['cycle']}/{doc['data_session']}\
                /user_data"
            )
            analysis_path = Path(
                f"/nsls2/data/smi/proposals/{doc['cycle']}/{doc['data_session']}\
                /analysis/{output_path}"
            )

        elif name == "resource":

            if doc["spec"] != "AD_TIFF":
                continue
            doc_root = doc["root"]
            resource_info[doc["uid"]] = {
                "path": Path(root_map.get(doc_root, doc_root)) / doc["resource_path"],  # noqa: 501
                "kwargs": doc["resource_kwargs"],
            }
        elif "datum" in name:
            if name == "datum":
                doc = event_model.pack_datum_page(doc)

            for datum_uid, point_number in zip(
                doc["datum_id"], doc["datum_kwargs"]["point_number"]
            ):
                datum_info[datum_uid] = (
                    resource_info[doc["resource"]],
                    point_number,
                )

        elif name == "descriptor":
            for k, v in doc["data_keys"].items():
                if "external" in v:
                    target_keys.add(k)
        elif "event" in name:
            # continue building the target_template here adding
            # the event level things (motor positions)
            if name == "event":
                doc = event_model.pack_event_page(doc)

            for key in target_keys:

                det, _, _ = key.partition("_")
                det_name = det.removeprefix("pil")
                det_type = det_map.get(det_name, det_name)

                if key not in doc["data"]:
                    continue

                for datum_id in doc["data"][key]:
                    # pulling out the image column
                    resource_vals, point_number = datum_info[datum_id]
                    orig_template = resource_vals["kwargs"]["template"]
                    fpp = resource_vals["kwargs"]["frame_per_point"]
                    base_fname = resource_vals["kwargs"]["filename"]

                    for fr in range(fpp):
                        source_path = Path(
                            orig_template
                            % (
                                str(resource_vals["path"]) + "/",
                                base_fname,
                                point_number * fpp + fr,
                            )
                        )

                        dest_path = target_path / target_template.format(
                            det_name=det_name,
                            N=point_number * fpp + fr,
                            det_type=det_type,
                        )
                        links.append(
                            (start_uid, source_path, dest_path, analysis_path)
                        )

        elif name == "stop":
            break

    linked, failed = do_symlinking(links, overwrite_dest=True)

    if len(failed) > 0:
        logger.info(f"Failed generating links {failed}")
        return
    elif len(linked) > 0:
        logger.info(f"Links successfully generated {linked}")
        return


@flow(log_prints=True)
def linker(ref):

    logger = get_run_logger()
    logger.info("Start linker...")

    det_map = {"900KW": "WAXS", "1M": "SAXS"}

    get_symlink_pairs(ref, det_map=det_map)

    logger.info("Finish linker.")
