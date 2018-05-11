src_dir=/home/classify/workspace/material_recommendation/log/
target_dir=/home/classify/workspace/material_recommendation/info_log/
src_filename=info_logger.log-`date -d "yesterday" +"%Y%m%d"`
target_filename=`hostname`_`date -d "yesterday" +"%Y%m%d"`_info_logger.log

scp $src_dir$src_filename classify@docker01:$target_dir$target_filename