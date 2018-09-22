#!/bin/bash
rm -rf cache
mkdir -p cache
yes | quilt install timkpaine/hedgeme 
quilt export timkpaine/hedgeme cache