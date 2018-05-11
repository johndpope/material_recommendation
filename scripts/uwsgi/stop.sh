#!/bin/sh

uwsgixml="`pwd`/uwsgi.xml"

pkill -9 -f ${uwsgixml}
