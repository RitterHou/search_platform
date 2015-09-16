#!/bin/sh

cd /opt/search_platform
git clone -b production http://git.dev.qianmi.com/commons/search_platform.git
cp search_platform listener
cp search_platform measure
cp search_platform suggest
cp search_platform supervisor
cp search_platform webserver
cp search_platform worker