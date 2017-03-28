#!/bin/bash
#unxz es.deduped.xz
mkdir in
mkdir out
split --bytes=3G es.deduped /in/f
ls in > files.txt
