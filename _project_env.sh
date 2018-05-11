#
# 设置项目的名字PROJECT_NAME
#
# 运维约定:
#
#
# 1. 项目的部署路径:
#    /home/account_name/workspace/PROJECT_NAME
#    /home/account_name/workspace/PROJECT_NAME_xxx
#    默认PROJECT_NAME和 _project_env.sh 文件中的设置一致，如果不一致，以 _project_env.sh中的配置为准，通过它来寻找对应的ENV
#
#    项目依赖的运行环境:
#    /home/account_name/workspace/ENV_PROJECT_NAME
#
# 2. 为了实现项目负责人和运维联合运维，独立的项目使用独立的账号运行, 例如: community, medical, fileupload等
#    以上的路径即为: /home/community/workspace/medweb
#                  /home/community/workspace/ENV_PROJECT_NAME
#
# 3. PYTHON.sh等脚本也通过PROJECT_NAME寻找和当前项目在同一个Parent目录中的虚拟环境, 不同项目的环境严格分离
#
#
# 4. 跑脚本时: 不要直接引用python, 而是通过
#   ./PYTHON.sh
#   ./PYTHON_DB.sh
#   ./PYTHON_SHELL.sh
#   ./MANAGE.sh
#   来自动定位python, 启动相关的程序
#
PROJECT_NAME=material_recommendation
PROJECT_USER=classify
