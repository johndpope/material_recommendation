./PYTHON.sh recommend/daily_scripts/topn_news.py get_uid > daily_scripts_cal_news.log 2>&1
date
nohup ./PYTHON.sh recommend/daily_scripts/topn_news.py cal_and_add 0 y >> daily_scripts_cal_news.log 2>&1 &

nohup ./PYTHON.sh recommend/daily_scripts/topn_news.py cal_and_add 1 a >> daily_scripts_cal_news.log 2>&1 &

sleep 1h
nohup ./PYTHON.sh recommend/daily_scripts/topn_news.py cal_and_add 2 n >> daily_scripts_cal_news.log 2>&1 &

nohup ./PYTHON.sh recommend/daily_scripts/topn_news.py cal_and_add 3 g >> daily_scripts_cal_news.log 2>&1 &
date