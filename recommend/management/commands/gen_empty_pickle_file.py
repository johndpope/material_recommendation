# -*- coding:utf-8 -*-
import os

from django.core.management.base import BaseCommand
from general_utils.file_utils import pickle_to_file


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        ./PYTHON.sh manage.py gen_empty_pickle_file
        """
        file_name = args[0]
        pickle_to_file({}, file_name)
