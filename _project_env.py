# -*- coding:utf-8 -*-
import os

PROJECT_NAME=""
PROJECT_USER=""

PROJECT_ROOT = os.path.dirname(__file__)

lines = open(os.path.join(PROJECT_ROOT, "_project_env.sh")).readlines()
for line in lines:
    line = line.strip()
    if line.find("#") != -1:
        continue

    if line.startswith("PROJECT_NAME"):
        PROJECT_NAME = line.split("=")[1].strip()
    elif line.startswith("PROJECT_USER"):
        PROJECT_USER = line.split("=")[1].strip()