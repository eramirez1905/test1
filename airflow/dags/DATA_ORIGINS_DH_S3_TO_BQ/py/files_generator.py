# airflow\dags\DAG_FLOW_NAME\py\ aca va el script
import json
import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from DATA_ORIGINS_DH_S3_TO_BQ_CONFIG import *

main_code = """import airflow
import json
import os, sys, inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append(currentdir)
# from OPSLOG_PANDORA_S3_TO_DL_DWH_CONFIG import *
from OPSLOG_PANDORA_S3_TO_DL_DWH_CORE import *

with open(currentdir + '/config/dag_bag_config.json') as json_file:
    dag_bag = json.load(json_file)
    
d = dag_bag['@key']
if d['active'] == True:
    dag_id = dag_name.format(d['model'])
    print("--->Generating DAG {0}".format(dag_name))
    globals()[dag_id] = createDAGFull(d, dag_id)"""

# def build():
#     dir = "../dag_{0}.py"
#     flow = os.path.basename(parentdir)
#     code_template = main_code.replace('@flow', flow)
#
#     with open('../config/dag_bag_config.json', 'w') as fp:
#         json.dump(dag_bag, fp)
#
#     for key in dag_bag:
#         d = dag_bag[key]
#         print(key)
#         codo_final = code_template.replace('@key', key)
#         file_path = dir.format(key)
#         if os.path.exists(file_path):
#             os.remove(file_path)
#         text_file = open(file_path, "w")
#         text_file.write(codo_final)
#         text_file.close()

def config():
    with open('../config/dag_bag_config.json', 'w') as fp:
        json.dump(dag_bag, fp)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        globals()[sys.argv[1]]()
    else:
        pass