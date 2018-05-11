./PYTHON.sh recommend/daily_scripts/topn_topics.py get_uid
./PYTHON.sh recommend/daily_scripts/topn_topics.py cal 0
./PYTHON.sh add_data_to_solr/manager/user_topn_topics.py add 0
./PYTHON.sh recommend/daily_scripts/topn_topics.py cal 1
./PYTHON.sh add_data_to_solr/manager/user_topn_topics.py add 1
./PYTHON.sh recommend/daily_scripts/topn_topics.py cal 2
./PYTHON.sh add_data_to_solr/manager/user_topn_topics.py add 2
./PYTHON.sh recommend/daily_scripts/topn_topics.py cal 3
./PYTHON.sh add_data_to_solr/manager/user_topn_topics.py add 3