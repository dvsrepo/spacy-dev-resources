#!/bin/bash
#unxz es.deduped.xz
mkdir ~/in
mkdir ~/out
split --bytes=3G ~/es.deduped ~/in/f
ls -d -1 ~/in/** > ~/files.txt
