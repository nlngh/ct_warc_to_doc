#!/bin/bash

# install python 3
yes | sudo yum install python3.7

# install git
yes | sudo yum install git

# clone git repo
git clone https://github.com/nlngh/ct_warc_to_doc.git

cd ct_warc_to_doc/

sudo pip3 install -r requirements.txt

yes | sudo yum install tmux
