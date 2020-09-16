#!/bin/bash

# install python 3
yes | sudo yum install python3.7

# install git
y | sudo yum install git

# clone git repo
git clone https://github.com/nlngh/warc_parser.git

cd warc_parser/

sudo pip3 install -r requirements.txt

y | sudo yum install tmux
