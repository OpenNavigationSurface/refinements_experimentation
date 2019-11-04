import logging
import os

import h5py

# setup logging

logging.basicConfig(level=logging.INFO, format="%(levelname)-9s %(name)s.%(funcName)s:%(lineno)d > %(message)s")
logger = logging.getLogger(__name__)

# retrieve the local test/data folder (for inputs)

test_data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "test", "data"))
if not os.path.exists(test_data_folder):
    raise RuntimeError("Unable to locate the test data folder: %s" % test_data_folder)
logger.info("test data folder: %s" % test_data_folder)

# create/retrieve the local test/output folder (for outputs)

test_output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "test", "output"))
if not os.path.exists(test_output_folder):
    os.mkdir(test_output_folder)
logger.info("test output folder: %s" % test_output_folder)

# retrieve the list of BAG files in the test/data folder

bag_paths = list()
for root, _, files in os.walk(test_data_folder):
    for f in files:
        if f.endswith(".bag"):
            bag_paths.append(os.path.join(root, f))
logger.info("nr. of available BAG files: %d" % len(bag_paths))

# select an input from the list of BAG files

bag_path = bag_paths[0]  # change this index to select another bag file
if not h5py.is_hdf5(bag_path):
    raise RuntimeError("The passed BAG file is not recognized as a valid HDF5 format")
logger.info("input BAG file: %s" % bag_path)

# open the input BAG in reading mode (and check the presence of the BAG_root group)

fid = h5py.File(bag_path, 'r')
try:
    fid["BAG_root"]
except KeyError:
    raise RuntimeError("The passed BAG file is not a valid HDF5 format: missing BAG_root group")
logger.info("input BAG: open")

# open the output BAG in writing mode

bag_name = os.path.basename(bag_path)
out_path = os.path.join(test_output_folder, os.path.splitext(bag_name)[0] + "_GSC" + os.path.splitext(bag_name)[1])
logger.info("output BAG file: %s" % out_path)
if os.path.exists(out_path):
    os.remove(out_path)
fod = h5py.File(out_path, 'w')
logger.info("output BAG: open")


# copy the elements in the input BAG that are not VR related


def clone_content_without_varres_items(key):

    # skip keys with 'varres' in the path
    if "varres" in key:
        logger.info("- %s: skip" % (key,))
        return

    # copy groups with attributes
    if isinstance(fid[key], h5py.Group):
        fod.create_group(key)
        for ka, kv in fid[key].attrs.items():
            fod[key].attrs[ka] = kv
            logger.info("- %s: group attribute copy: %s -> %s" % (key, ka, kv))
        logger.info("- %s: group copy" % (key,))
        return

    # copy datasets with attributes
    if isinstance(fid[key], h5py.Dataset):
        fod.create_dataset(key, data=fid[key])
        for ka, kv in fid[key].attrs.items():
            fod[key].attrs[ka] = kv
            logger.info("- %s: dataset attribute copy: %s -> %s" % (key, ka, kv))
        logger.info("- %s: dataset copy (%s)" % (key, fid[key].dtype))


logger.info("cloning content (skipping varres* elements)")
fid.visit(clone_content_without_varres_items)

#  create the BAG_tiles sub-group to store the tiles for the corresponding super cells

bag_tiles_group = "BAG_root/BAG_tiles"
fod.create_group(bag_tiles_group)
logger.info("output BAG: created %s" % bag_tiles_group)

# convert the list of refinements in the input BAG to tiles in the output BAG

valid_tiles = dict()


def modify_varres_content(key):

    # skip keys not containing 'varres'
    if "varres" not in key:
        logger.info("- %s: skip" % (key,))
        return

    # retrieve and store the metadata relative to the VR refinements
    # + create a tile for each super cell with VR refinements
    if "varres_metadata" in key:
        meta = fid[key]
        logger.info("- %s -> %s" % (key, meta.shape))
        group_counter = 0
        for r in range(meta.shape[0]):
            for c in range(meta.shape[1]):
                if meta[r][c][-1] != -1:
                    logger.info("- valid tile (%s, %s): %s" % (r, c, meta[r][c]))
                    valid_tiles[(r, c)] = meta[r][c]
                    tile_group = bag_tiles_group + "/%d_%d" % (r, c)
                    fod.create_group(tile_group)
                    fod[tile_group].attrs["dimensions_x"] = meta[r][c][1]
                    fod[tile_group].attrs["dimensions_y"] = meta[r][c][2]
                    fod[tile_group].attrs["resolution_x"] = meta[r][c][3]
                    fod[tile_group].attrs["resolution_y"] = meta[r][c][4]
                    fod[tile_group].attrs["sw_corner_x"] = meta[r][c][5]
                    fod[tile_group].attrs["sw_corner_y"] = meta[r][c][6]
                    fod[tile_group].attrs["group_id"] = group_counter
                    group_counter += 1
        return

    # convert the refinements in the input BAG to tiles for each super cell
    if "varres_refinements" in key:
        refs = fid[key][0]
        logger.info("- %s -> %s" % (key, refs.shape))

        for idx, meta in valid_tiles.items():
            tile_group = bag_tiles_group + "/%d_%d" % idx
            logger.info("- populating tile: %s" % tile_group)
            to = meta[0]

            tile_elevation = tile_group + "/elevation"
            fod.create_dataset(tile_elevation, (meta[1], meta[2]), dtype="float32")
            for tr in range(meta[2]):
                for tc in range(meta[1]):
                    fod[tile_elevation][tr, tc] = refs[to + tr * meta[1] + tc][0]

            tile_tracking_list = tile_group + "/tracking_list"
            fod.create_dataset(tile_tracking_list, (0, 0),
                               dtype={'names': ['row', 'col', 'depth', 'uncertainty', 'track_code', 'list_series'],
                                      'formats': ['<u4', '<u4', '<f4', '<f4', 'u1', '<i2'],
                                      'offsets': [0, 4, 8, 12, 16, 18], 'itemsize': 20})

            tile_uncertainty = tile_group + "/uncertainty"
            fod.create_dataset(tile_uncertainty, (meta[1], meta[2]), dtype="float32")
            for tr in range(meta[2]):
                for tc in range(meta[1]):
                    fod[tile_uncertainty][tr, tc] = refs[to + tr * meta[1] + tc][1]

    # take care of the values in the VR tracking list (currently, not implemented)
    if "varres_tracking_list" in key:
        trk = fid[key]
        logger.info("- %s -> %s" % (key, trk.shape))
        if trk.shape[0] != 0:
            logger.warning("reading of varres_tracking_list NOT implemented")


logger.info("modifying varres content")
fid.visit(modify_varres_content)
