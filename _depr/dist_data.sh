#!/bin/bash
find cache/ -name "*-*" | xargs rm -rf
find cache/ -name "*+" | xargs rm -rf
find cache/ -name "*=" | xargs rm -rf
find cache/ -name "*^" | xargs rm -rf
quilt build timkpaine/hedgeme cache
quilt push timkpaine/hedgeme --public
