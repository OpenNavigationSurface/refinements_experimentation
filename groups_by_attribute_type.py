import logging
import os

import h5py
from lxml import etree

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
out_path = os.path.join(test_output_folder, os.path.splitext(bag_name)[0] + "_ATT" + os.path.splitext(bag_name)[1])
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

#  create the BAG_tiles root-group to store the tiles for the corresponding super cells

bag_tiles_group = "BAG_tiles"


def create_bag_tiles_group():

    fod.create_group(bag_tiles_group)
    logger.info("output BAG_tiles: created %s" % bag_tiles_group)

    # copy BAG version
    fod["BAG_tiles"].attrs.create("Bag Version", fid["BAG_root"].attrs["Bag Version"], shape=(), dtype="S5")

    # retrieve metadata
    ns = {
        'bag': 'http://www.opennavsurf.org/schema/bag',
        'gco': 'http://www.isotc211.org/2005/gco',
        'gmd': 'http://www.isotc211.org/2005/gmd',
        'gmi': 'http://www.isotc211.org/2005/gmi',
        'gml': 'http://www.opengis.net/gml/3.2',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    }
    ns2 = {
        'gml': 'http://www.opengis.net/gml',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'smXML': 'http://metadata.dgiwg.org/smXML',
    }

    # retrieve/write CRSs
    xml_tree = etree.fromstring(fid["BAG_root/metadata"][:].tostring())
    # bag_metadata = etree.tostring(xml_tree, pretty_print=True)
    # logger.info("metadata: %s" % bag_metadata)
    crs = xml_tree.xpath('//*/gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/'
                         'gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString',
                         namespaces=ns)
    if len(crs) == 0:
        try:
            crs = xml_tree.xpath('//*/referenceSystemInfo/smXML:MD_CRS',
                                 namespaces=ns2)
        except etree.Error as e:
            logger.warning("unable to read the WKT projection string: %s" % e)
            return
    # logger.info("crs: %s" % crs[0].text)
    fod["BAG_tiles"].attrs["crs_horizontal"] = crs[0].text
    fod["BAG_tiles"].attrs["crs_vertical"] = crs[1].text

    # attempts to read rows and cols info
    try:
        shape = xml_tree.xpath('//*/gmd:spatialRepresentationInfo/gmd:MD_Georectified/'
                               'gmd:axisDimensionProperties/gmd:MD_Dimension/gmd:dimensionSize/gco:Integer',
                               namespaces=ns)
    except etree.Error as e:
        logger.warning("unable to read rows and cols: %s" % e)
        return
    if len(shape) == 0:
        try:
            shape = xml_tree.xpath('//*/spatialRepresentationInfo/smXML:MD_Georectified/'
                                   'axisDimensionProperties/smXML:MD_Dimension/dimensionSize',
                                   namespaces=ns2)
        except etree.Error as e:
            logger.warning("unable to read rows and cols: %s" % e)
            return
    fod["BAG_tiles"].attrs["supergrid_rows"] = shape[0].text
    fod["BAG_tiles"].attrs["supergrid_columns"] = shape[1].text

    # attempts to read resolution along x- and y- axes
    try:
        res = xml_tree.xpath('//*/gmd:spatialRepresentationInfo/gmd:MD_Georectified/'
                             'gmd:axisDimensionProperties/gmd:MD_Dimension/gmd:resolution/gco:Measure',
                             namespaces=ns)
    except etree.Error as e:
        logger.warning("unable to read res x and y: %s" % e)
        return
    if len(res) == 0:
        try:
            res = xml_tree.xpath('//*/spatialRepresentationInfo/smXML:MD_Georectified/'
                                 'axisDimensionProperties/smXML:MD_Dimension/resolution/'
                                 'smXML:Measure/smXML:value',
                                 namespaces=ns2)
        except etree.Error as e:
            logger.warning("unable to read res x and y: %s" % e)
            return
    try:
        fod["BAG_tiles"].attrs["supergrid_res_x"] = float(res[0].text)
        fod["BAG_tiles"].attrs["supergrid_res_y"] = float(res[1].text)

    except (ValueError, IndexError) as e:
        logger.warning("unable to read res x and y: %s" % e)
        return

    # attempts to read corners SW and NE
    try:
        coords = xml_tree.xpath('//*/gmd:spatialRepresentationInfo/gmd:MD_Georectified/'
                                'gmd:cornerPoints/gml:Point/gml:coordinates',
                                namespaces=ns)[0].text.split()
    except (etree.Error, IndexError) as e:
        try:
            coords = xml_tree.xpath('//*/spatialRepresentationInfo/smXML:MD_Georectified/'
                                    'cornerPoints/gml:Point/gml:coordinates',
                                    namespaces=ns2)[0].text.split()
        except (etree.Error, IndexError) as e:
            logger.warning("unable to read corners SW and NE: %s" % e)
            return

    try:
        fod["BAG_tiles"].attrs["supergrid_south"] = [float(c) for c in coords[0].split(',')][1]
        fod["BAG_tiles"].attrs["supergrid_west"] = [float(c) for c in coords[0].split(',')][0]

    except (ValueError, IndexError) as e:
        logger.warning("unable to read corners SW and NE: %s" % e)
        return

    # copy the metadata with attributes
    key = "BAG_tiles/metadata"
    fod.create_dataset(key, data=fid["BAG_root/metadata"])
    for ka, kv in fid["BAG_root/metadata"].attrs.items():
        fod[key].attrs[ka] = kv
        logger.info("- %s: dataset attribute copy: %s -> %s" % (key, ka, kv))
    logger.info("- %s: dataset copy (%s)" % (key, fid["BAG_root/metadata"].dtype))


create_bag_tiles_group()

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
        fod.create_dataset(bag_tiles_group + "/res_x"   , meta.shape, dtype="float32")
        fod.create_dataset(bag_tiles_group + "/res_y"   , meta.shape, dtype="float32")
        fod.create_dataset(bag_tiles_group + "/west"    , meta.shape, dtype="float32")
        fod.create_dataset(bag_tiles_group + "/south"   , meta.shape, dtype="float32")
        fod.create_dataset(bag_tiles_group + "/group_id", meta.shape, dtype="int")
        for r in range(meta.shape[0]):
            for c in range(meta.shape[1]):
                if meta[r][c][-1] != -1:
                    logger.info("- valid tile (%s, %s): %s" % (r, c, meta[r][c]))
                    valid_tiles[(r, c)] = meta[r][c]
                    
                    fod[bag_tiles_group + "/res_x"][r,c] = meta[r][c][3]
                    fod[bag_tiles_group + "/res_y"][r,c] = meta[r][c][4]
                    fod[bag_tiles_group + "/west"][r,c] = fod["BAG_tiles"].attrs["supergrid_west"] \
                                                    + c * fod["BAG_tiles"].attrs["supergrid_res_x"] \
                                                    + meta[r][c][5]
                    fod[bag_tiles_group + "/south"][r,c] = fod["BAG_tiles"].attrs["supergrid_south"] \
                                                     + r * fod["BAG_tiles"].attrs["supergrid_res_y"] \
                                                     + meta[r][c][6]
                    fod[bag_tiles_group + "/group_id"][r,c] = group_counter  # added group_id for clustering tiles
                    group_counter += 1
        return

    # convert the refinements in the input BAG to tiles for each super cell
    if "varres_refinements" in key:
        refs = fid[key][0]
        logger.info("- %s -> %s" % (key, refs.shape))

        # retrieve tracking list to evaluate its number of elements
        trk = fid["BAG_root/varres_tracking_list"]
        logger.info("- %s -> %s" % (key, trk.shape))
        
        # create the attribute groups
        elevation_group = bag_tiles_group + "/elevation"
        fod.create_group(elevation_group)
        uncert_group = bag_tiles_group + "/uncertainty"
        fod.create_group(uncert_group)
        tracking_group = bag_tiles_group + "/tracking_list"
        if trk.shape[0] != 0:
            fod.create_group(tracking_group)

        for idx, meta in valid_tiles.items():
            tile_id = "/%d_%d" % idx
            logger.info("- populating tile: %s -> [%s]" % (bag_tiles_group + tile_id, meta))
            to = meta[0]

            tile_elevation = elevation_group + tile_id
            fod.create_dataset(tile_elevation, (meta[2], meta[1]), dtype="float32")
            for tr in range(meta[2]):
                for tc in range(meta[1]):
                    fod[tile_elevation][tr, tc] = refs[to + tr * meta[2] + tc][0]

            if trk.shape[0] != 0:
                tile_tracking_list = tracking_group + tile_id
                fod.create_dataset(tile_tracking_list, (0, 0),
                                   dtype={'names': ['row', 'col', 'depth', 'uncertainty', 'track_code', 'list_series'],
                                          'formats': ['<u4', '<u4', '<f4', '<f4', 'u1', '<i2'],
                                          'offsets': [0, 4, 8, 12, 16, 18], 'itemsize': 20})

            tile_uncertainty = uncert_group + tile_id
            fod.create_dataset(tile_uncertainty, (meta[2], meta[1]), dtype="float32")
            for tr in range(meta[2]):
                for tc in range(meta[1]):
                    fod[tile_uncertainty][tr, tc] = refs[to + tr * meta[2] + tc][1]

    # take care of the values in the VR tracking list (currently, not implemented)
    if "varres_tracking_list" in key:
        trk = fid[key]
        if trk.shape[0] != 0:
            logger.warning("reading of varres_tracking_list NOT implemented")


logger.info("modifying varres content")
fid.visit(modify_varres_content)
