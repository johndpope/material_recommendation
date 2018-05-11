local_path=/home/classify/workspace/material_recommendation/data_dir/user_topn_topic/
local_src1=20171227.uid2
local_src2=20171227.uid3
remote_path=/home/classify/workspace/material_recommendation/data_dir/user_topn_topic/

scp $local_path$local_src1 classify@learn:$remote_path
scp $local_path$local_src2 classify@learn:$remote_path