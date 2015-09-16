#!/bin/sh

cd /opt/search_platform
git clone -b production http://git.dev.qianmi.com/commons/search_platform.git
cp search_platform management
cp search_platform supervisor
cp search_platform webserver