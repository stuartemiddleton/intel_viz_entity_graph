# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
/////////////////////////////////////////////////////////////////////////
//
// (c) Copyright University of Southampton 2020
//
// This software may not be used, sold, licensed, transferred, copied
// or reproduced in whole or in part in any manner or form or in or
// on any media by any person other than in accordance with the terms
// of the Licence Agreement supplied with the software, or otherwise
// without the prior written consent of the copyright owners.
//
// This software is distributed WITHOUT ANY WARRANTY, without even the
// implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE, except where stated in the Licence Agreement supplied with
// the software.
//
// Created By :         Stuart E. Middleton
// Created Date :       2020/07/02
// Created for Project: FloraGuard
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////
"""

import os, sys, logging, traceback, codecs, datetime, copy, time, ast, math, re, random, shutil, json, csv, multiprocessing, subprocess
import intel_viz_lib


################################
# main
################################

# only execute if this is the main file
if __name__ == '__main__' :

	#
	# check args
	#
	if len(sys.argv) < 3 :
		print('Usage: intel_viz_lib.py <config_file> <data_graph>')
		sys.stdout.flush()
		sys.exit(1)

	# make logger (global to STDOUT)
	LOG_FORMAT = ('%(levelname) -s %(asctime)s %(message)s')
	logger = logging.getLogger( __name__ )
	logging.basicConfig( level=logging.INFO, format=LOG_FORMAT )
	logger.info('started')

	try :
		# init
		strConfigFile = sys.argv[1]
		if not os.path.isfile(strConfigFile) :
			print('<config_file> ' + strConfigFile + ' does not exist\n')
			sys.stdout.flush()
			sys.exit(1)

		strDataGraphFile = sys.argv[2]
		if not os.path.isfile(strDataGraphFile) :
			print('<data_graph> ' + strDataGraphFile + ' does not exist\n')
			sys.stdout.flush()
			sys.exit(1)

		logger.info('data_graph: ' + repr(strDataGraphFile) )

		# load config
		logger.info('config_file: ' + repr(strConfigFile) )
		dictAppConfig = intel_viz_lib.read_config( strConfigFile )
		dictAppConfig['logger'] = logger

		dictEntityIndex, listRootNodes = intel_viz_lib.load_data_graph(
			data_graph_file = strDataGraphFile,
			dict_config = dictAppConfig )

		intel_viz_lib.viz_data_graph(
			list_root_nodes = listRootNodes,
			entity_index = dictEntityIndex,
			dict_config = dictAppConfig )

	except :
		logger.exception( 'intel_viz main() exception' )
		sys.stderr.flush()
		sys.stdout.flush()

		sys.stdout.flush()
		sys.exit(1)

	# all done
	logger.info('finished')
	sys.stderr.flush()
	sys.stdout.flush()
	sys.exit(0);
