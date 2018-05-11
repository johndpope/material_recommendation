import os
from global_config import get_root_path

CF_DATA_DIR = os.path.join(get_root_path(), 'cf', 'cf_data_dir')

MATERIAL_CLICK_USRES_FILE = os.path.join(CF_DATA_DIR, 'material_click_users.json')
MATERIAL_SORTED_NEIGHBOUR = os.path.join(CF_DATA_DIR, 'material_neighbour.json')
