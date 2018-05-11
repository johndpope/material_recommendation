# -*- coding:utf-8 -*-
import os

from django.core.management.base import BaseCommand
from rpc_thrift.config import parse_config
from rpc_thrift.worker import RpcWorker

import global_config
from material_recommendation_service.MaterialRecommendationService import Processor
from rpc_services.rpc_processor import MaterialRecommendationProcessor


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        ./PYTHON.sh manage.py run_thrift_server
        """
        processor = Processor(MaterialRecommendationProcessor())
        #MedicalProcessor.timer(60)

        config_path = os.path.join(global_config.get_root_path(), 'config.ini')
        config = parse_config(config_path)

        endpoint = config["back_address"]
        service = config["service"]
        worker_pool_size = int(config["worker_pool_size"])

        s = RpcWorker(processor, endpoint, pool_size=worker_pool_size, service=service)
        s.run()
