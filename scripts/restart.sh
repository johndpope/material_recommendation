sh control_lb.sh stop
sh scripts/sup_stop.sh
sleep 3
sh scripts/sup_start.sh
sleep 10
sh control_lb.sh start
sleep 5
sh control_lb.sh status