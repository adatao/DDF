#!/bin/bash

#
# To be sourced by run scripts.
#
# Determines if we should run in local or distributed mode, based on the first argument
# being -local or -distributed. If this argument isn't provided, tries to make some
# heuristic decision about whether we should be local or distributed, defaulting to
# "distributed", since we may have automated scripts that aren't yet updated to include
# this argument.
#

function determine_server_mode {
	local SERVER_MODE=""

	[ "$1" == "-l" -o "$1" == "--local" ] && SERVER_MODE="local"
	[ "$1" == "-d" -o "$1" == "--distributed" ] && SERVER_MODE="distributed"

	if [ "$SERVER_MODE" == "" ] ; then
		local dir="`dirname ${BASH_SOURCE[0]}`"
		dir="`cd $dir 2>&1 >/dev/null ; pwd`"

		[ "$SERVER_MODE" == "" ] && [[ $dir =~ ^/Volumes ]] && SERVER_MODE=local
		[ "$SERVER_MODE" == "" ] && [[ $dir =~ ^/root ]] && SERVER_MODE=distributed
	fi

	if [ "$SERVER_MODE" == "" ] ; then
		curl -s -f -m 1 http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null >/dev/null
		if [ $? == 0 ] ; then
			# Running on EC2
			SERVER_MODE=distributed
		else
			# Not running on EC2
			SERVER_MODE=local
		fi
	fi

	# Default to distributed if not yet set
	SERVER_MODE=${SERVER_MODE-"distributed"}
	
	echo $SERVER_MODE
}

determine_server_mode $@
