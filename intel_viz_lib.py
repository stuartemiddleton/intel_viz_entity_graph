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

import os, sys, logging, traceback, codecs, datetime, copy, time, ast, math, re, random, shutil, json, csv, multiprocessing, subprocess, configparser, hashlib
import networkx as nx
import matplotlib.pyplot as plt

def read_config( filename, logger = None ) :
	"""
	read config file using ConfigParser.ConfigParser(). this helper function processes lists, tuples and dictionary serializations using ast.literal_eval() to return python objects not str

	:param str filename: config ini filename
	:param logging.Logger logger: logger object
	:return: parsed config file in a dict structure e.g. { 'database_name' : 'mydatabase', 'list_names' : ['name1','name2'] }
	:rtype: dict
	"""

	if (not isinstance( filename, str )) and (not isinstance( filename, str )) :
		raise Exception( 'filename invalid' )

	if logger == None :
		logger = logging.getLogger()

	logger.info( 'reading config file : ' + str(filename) )
	dictConfig = {}
	Config = configparser.ConfigParser()
	Config.read( filename )
	listSections = Config.sections()
	for section in listSections :
		logger.info( '[' + str(section) + ']' )
		listOptions = Config.options(section)
		for option in listOptions :
			dictConfig[option] = Config.get(section, option)

			# use python to decode lists and dict entries
			if len(dictConfig[option]) > 1 :
				if (dictConfig[option][0] == '[') or (dictConfig[option][0] == '{') or (dictConfig[option][0] == '(') :
					dictConfig[option] = ast.literal_eval( dictConfig[option] )

			logger.info( '  ' +str(option) + '= ' + str(dictConfig[option]) )

	return dictConfig

def load_data_graph( data_graph_file = None, dict_config = None ) :
	"""
	load data graph, cluster and index all entities within it ready for visualization

	:param str data_graph_file: filename of data graph (JSON formatted)
	:param dict dict_config: config object containing root node spec and filters
	:return: entity index, root node list
	:rtype: dict, list
	"""

	if not isinstance( dict_config, dict) :
		raise Exception( 'dict_config invalid : ' + repr(dict_config) )
	if not isinstance( data_graph_file, str) :
		raise Exception( 'data graph data_graph_file invalid : ' + repr(data_graph_file) )
	if not os.path.exists( data_graph_file ):
		raise Exception( 'data graph filename does not exist : ' + repr(data_graph_file) )

	dictEntityIndex = index_intel_data(
		file_json = data_graph_file,
		dict_config = dict_config )

	dict_config['logger'].info( 'index entities (source) # ' + str(len(dictEntityIndex)) )

	#dict_config['logger'].info('T1 = ' + json.dumps(dictEntityIndex,indent=True) )

	listRootNodes_initial = generate_root_node_list(
		entity_index = dictEntityIndex,
		dict_config = dict_config )

	dict_config['logger'].info( 'root nodes (source) # ' + str(len(listRootNodes_initial)) )

	#dict_config['logger'].info('T2 = ' + json.dumps(listRootNodes_initial,indent=True) )

	dictClusteredEntityIndex = cluster_index(
		entity_index = dictEntityIndex,
		list_root_nodes = listRootNodes_initial,
		dict_config = dict_config )

	dict_config['logger'].info( 'clusters # ' + str(len(dictClusteredEntityIndex)) )

	#dict_config['logger'].info('T3 = ' + json.dumps(dictClusteredEntityIndex,indent=True) )

	listRootNodes_cluster = generate_root_node_list(
		entity_index = dictClusteredEntityIndex,
		dict_config = dict_config )

	dict_config['logger'].info( 'root nodes (post clustering) # ' + str(len(listRootNodes_cluster)) )

	#dict_config['logger'].info('T4 = ' + json.dumps(listRootNodes_cluster,indent=True) )

	dictFilteredEntityIndex = copy.deepcopy( dictClusteredEntityIndex )
	for dictFilterSpec in dict_config['filter_spec'] :
		dictFilteredEntityIndex = filter_index(
			entity_index = dictFilteredEntityIndex,
			list_root_nodes = listRootNodes_cluster,
			filter_spec = dictFilterSpec,
			dict_config = dict_config )

	dict_config['logger'].info( 'index entities (post filtering) # ' + str(len(dictFilteredEntityIndex)) )

	#dict_config['logger'].info('T5 = ' + json.dumps(dictFilteredEntityIndex,indent=True) )

	return dictFilteredEntityIndex, listRootNodes_cluster

def viz_data_graph( list_root_nodes = [], entity_index = {}, dict_config = None ) :
	"""
	visualize the data graph as a matplotlib interactive figure (that can be saved to disk if needed)

	:param list list_root_nodes: list of root node entities
	:param dict entity_index: entity index created by load_data_graph()
	:param dict dict_config: config object containing root node spec and filters
	"""

	search_depth = int( dict_config['search_depth'] )
	filter_post_freq = ast.literal_eval( dict_config['filter_post_freq'] )
	list_direction = dict_config['list_direction']
	layout_name = dict_config['layout_name']
	max_nodes = int( dict_config['max_nodes'] )
	list_pseudonymization = dict_config['list_pseudonymization']
	aggregate_nodes = True

	# change current (default) figure size to be the screen size for a large display
	screen_y = plt.get_current_fig_manager().window.winfo_screenheight()
	screen_x = plt.get_current_fig_manager().window.winfo_screenwidth()
	dict_config['logger'].info( 'screen size = ' + repr( (screen_x, screen_y) ) )
	plt.gcf().set_size_inches( 0.8*screen_x/96, 0.8*screen_y/96 )
	plt.gcf().set_dpi( 96 )

	# create networkx graph object which will do the actually rendering work
	G =  nx.Graph()
	for strRootNode in list_root_nodes:
		bfs(
			G,
			strRootNode,
			entity_index,
			search_depth = search_depth,
			list_direction = list_direction )
	
	#dict_config['logger'].info( 'graph nodes = ' + str(len(G)) )

	if aggregate_nodes == True :
		aggregate_nodes_with_same_base(
			G,
			entity_index = entity_index,
			root_node_list = list_root_nodes,
			filter_post_freq = filter_post_freq )

	dict_config['logger'].info( 'graph nodes after aggregation = ' + str(len(G)) )

	colour_map = dict_config['colour_map']
	nx.set_node_attributes( G, name='category', values=colour_map )

	for strEntity in G.nodes() :
		if strEntity in list_root_nodes :
			G.nodes[strEntity]['category'] = 'root'
		else :
			bFound = False
			for strCategory in dict_config['entity_prefix_map'] :
				for strPrefix in dict_config['entity_prefix_map'][strCategory] :
					if strEntity.startswith( strPrefix ) :
						G.nodes[strEntity]['category'] = strCategory
						bFound = True
						break
				if bFound == True :
					break
			if bFound == False :
				G.nodes[strEntity]['category'] = 'unknown'

	# order nodes by connection density
	listOrderedNodes = []
	for strEntity in G.nodes() :
		# sum up the weights of each edge 
		nConnections = 0
		dictEdges = G[strEntity]
		for strNodeConnected in dictEdges :
			nConnections = nConnections + dictEdges[strNodeConnected]['weight']
		listOrderedNodes.append( ( strEntity,nConnections ) )
	listOrderedNodes = sorted( listOrderedNodes, key=lambda entry: entry[1], reverse=True )

	# remove all but top N nodes to avoid overloading the graph (which will be very slow to render)
	nRemovedCount = 0
	if G.number_of_nodes() > max_nodes :
		# remove nodes outside topN (and not a root node)

		nIndex1 = max_nodes
		while nIndex1 < len(listOrderedNodes) :
			( strEntityOrderedList, nConnectionsOrderedList ) = listOrderedNodes[ nIndex1 ]
			if not strEntityOrderedList in list_root_nodes :
				G.remove_node( strEntityOrderedList )
				del listOrderedNodes[ nIndex1 ]
				nRemovedCount += 1
			else :
				nIndex1 += 1

		dict_config['logger'].info( 'max nodes exceeded # ' + str(nRemovedCount) + ' nodes removed' )

	# make names and sizes for all nodes
	listNodeSizes = []
	dictNodeNames = {}
	listPseudonymization = dict_config['list_pseudonymization']

	for strEntity in G.nodes() :
		for ( strEntityOrderedList, nConnectionsOrderedList ) in listOrderedNodes :
			if strEntityOrderedList == strEntity :
				nConnections = nConnectionsOrderedList
				break

		if nConnections < 10 :
			nSize = 200
		elif nConnections < 20 :
			nSize = 400
		elif nConnections < 50 :
			nSize = 800
		else :
			nSize = 1600
		listNodeSizes.append( nSize )

		# pretty print entities
		if '@@@' in strEntity :
			strName = strEntity.split('@@@')[0]
		elif ':' in strEntity :
			strName = ':'.join(strEntity.split(':')[1:])
		else :
			strName = strEntity

		# pseudonymization (use hash of name prefixed by type)
		if (len(listPseudonymization) > 0) and (len(strName) > 0) :

			strCat = G.nodes[strEntity]['category']
			if strCat in listPseudonymization :
				strHashedName = hashlib.shake_256( strName.encode("utf-8") ).hexdigest( 2 )

				if strCat.startswith('entity_') :
					strName = strCat[ len('entity_') : ] + '_' + strHashedName
				elif strCat == 'root' :
					strName = 'target_' + strHashedName
				elif strName.startswith('thread[') :
					strName = 'thread_' + strHashedName
				else :
					strName = strCat + '_' + strHashedName

		# truncate long names (too log to fit like page URL's - 30 character limit)
		nTrunc = int( dict_config['max_node_text_length'] )
		if nTrunc != 0 :
			strName = strName[:nTrunc]

		# add name to dict
		dictNodeNames[strEntity] = strName

		# show NER types as prefix (normally would not do this, useful for debug)
		if 'preserve_node_prefix' in dict_config :
			if ast.literal_eval( dict_config['preserve_node_prefix'] ) == True :
				dictNodeNames[strEntity] = strEntity

	# layout by edge weight
	dictEdgeLabels = nx.get_edge_attributes( G, 'weight' )

	if layout_name == 'spring' :
		pos = nx.spring_layout( G, weight='weight', scale = 10 )
	elif layout_name == 'random' :
		pos = nx.random_layout( G )
	elif layout_name == 'shell' :
		listInside = []
		listOutside = []
		for strNode in G.nodes() :
			if strNode in list_root_nodes :
				listInside.append( strNode )
			else :
				listOutside.append( strNode )
		pos = nx.shell_layout( G, [ listInside, listOutside ] )
	elif layout_name == 'spectral' :
		pos = nx.spectral_layout( G )
	else :
		raise Exception( 'unknown layout : ' + repr(layout_name) )

	listNodeColours = []
	for (strNode,dictAttr) in G.nodes(data=True) :
		listNodeColours.append( colour_map[ dictAttr['category'] ] )

	listEdgeColours = []
	listEdgeLineWidths = []
	for ( strNode1,strNode2,dictAttr ) in G.edges(data=True) :
		strCat = G.nodes[ strNode1 ]['category']
		listEdgeColours.append( colour_map[ strCat ] )
		nWidth = dictAttr['weight']
		if nWidth > 5 :
			nWidth = 5
		listEdgeLineWidths.append( nWidth )

	nx.draw(G,
		pos,
		linewidths=1,
		node_size=listNodeSizes,
		alpha=0.9,
		font_size= 12,
		labels=dictNodeNames,
		node_color = listNodeColours,
		edge_color = listEdgeColours,
		width = listEdgeLineWidths,
		)


	nx.draw_networkx_edge_labels(
		G,
		pos,
		edge_labels = dictEdgeLabels,
		font_color='grey' )

	limits = plt.axis('off')  # turn off axis

	plt.show()




def index_intel_data( file_json = None, dict_config = {} ):
	"""
	load a JSON file with intelligence data and create a set of entity indexes

	:param unicode file_json: filename of JSON intelligence report to load
	:param dict dict_config: config object

	:return: entity index
	:rtype: dict
	"""

	readHandle = codecs.open( filename=file_json, mode='r', encoding='utf-8', errors='replace' )
	strTotalText = readHandle.read()
	readHandle.close()
	dictJSON = json.loads( strTotalText )

	dictEntityIndex = {}
	for strPostID in dictJSON :
		dictPost = dictJSON[strPostID]

		if not 'author' in dictPost :
			raise Exception( 'post with no author : ' + repr(strPostID) )
		strAuthor = dictPost['author']

		if not 'page_url' in dictPost :
			raise Exception( 'post with page URL : ' + repr(strPostID) )
		strPostURL = dictPost['page_url']

		strPostEntity = 'posts[' + strAuthor + ']@@@' + strPostID
		strAuthorEntity = 'NER-PERSON:' + strAuthor
		strPageURLEntity = 'PAGE-URL:' + strPostURL

		strThread = 'thread[unknown]'
		if 'thread_' in strPostID :
			strThread = strPostID[ strPostID.index('thread_') + len('thread_') : ]
			if '_' in strThread :
				strThread = strThread[ :strThread.index('_') ]
			strThread = 'thread[' + strThread + ']'

		# populate post level entity connections
		# author -> post
		if not strPostEntity in dictEntityIndex :
			dictEntityIndex[strPostEntity] = {}
		if not strAuthorEntity in dictEntityIndex :
			dictEntityIndex[strAuthorEntity] = {}
		dictEntityIndex[ strAuthorEntity ][ strPostEntity ] = 1

		# thread -> post
		if not strPostEntity in dictEntityIndex :
			dictEntityIndex[strPostEntity] = {}
		if not strThread in dictEntityIndex :
			dictEntityIndex[strThread] = {}
		dictEntityIndex[ strThread ][ strPostEntity ] = 1

		# post -> page_url
		dictEntityIndex[ strPostEntity ] = { strPageURLEntity : 1 }
		if not strPageURLEntity in dictEntityIndex :
			dictEntityIndex[ strPageURLEntity ] = {}

		# loop on sents in the post
		for strSentIndex in dictPost :
			if not strSentIndex in [ 'author', 'page_url' ] :

				# loop on each individual extraction
				for dictExtraction in dictPost[strSentIndex] :

					# process entities
					for strExtractKey in dictExtraction :
						if strExtractKey in ['entity'] :

							for strEntity in dictExtraction[strExtractKey] :

								strEntityLabel = strEntity
								if not strEntityLabel in dictEntityIndex :
									dictEntityIndex[ strEntityLabel ] = {}

								# post_<id> -> entity
								# thread_<id> -> entity
								if not strEntityLabel in dictEntityIndex[ strPostEntity ] :
									# freq count for linked entity to author
									dictEntityIndex[ strPostEntity ][ strEntityLabel ] = 1
									dictEntityIndex[ strThread ][ strEntityLabel ] = 1
								else  :
									# freq count for linked entity to author
									dictEntityIndex[ strPostEntity ][ strEntityLabel ] += 1
									dictEntityIndex[ strThread ][ strEntityLabel ] += 1

	# all done
	return dictEntityIndex

def generate_root_node_list( entity_index = None, dict_config = {} ):
	"""
	generate a root node list from the entity index

	:param dict entity_index: index created by load_data_graph()
	:param dict dict_config: config object containing root node spec and filters
	:return: root node entity list
	:rtype: list
	"""

	return entity_lookup_using_filter(
				entity_index = entity_index,
				filter_spec = dict_config['root_node_spec'],
				dict_config = dict_config
				)

def entity_lookup_using_filter( entity_index, filter_spec, dict_config = {} ) :
	"""
	filter entity index according to a filter spec

	:param dict entity_index: index created by load_data_graph()
	:param dict filter_spec: filter spec to apply
	:param dict dict_config: config object
	:return: list of entities in the index that survived the filter
	:rtype: list
	"""

	#filter_spec = {
	#	'match' : {
	#		'entity' : ['entity:elieestephane', 'entity:harald', 'entity:saussurea', 'entity:auklandia', 'entity:kuth', 'entity:kustha', 'entity:postkhai', 'entity:kostum', 'entity:sepuddy', 'entity:koshta', 'entity:aplotaxis lappa', 'entity:ariocarpus', 'entity:a agavoides', 'entity:a scapharostrus', 'entity:a scapharostroides', 'entity:a scaphirostris', 'entity:a retusus', 'entity:a trigonus', 'entity:a bravoanus', 'entity:a hintonii', 'entity:a kotschubeyanus', 'entity:a kotshoubeyanus', 'entity:a albiflorus', 'entity:a kotschobeyanus', 'entity:a kotsch', 'entity:tamaulipas living rock cactus', 'entity:nuevo leon living rock cactus', 'entity:euphorbia decaryi', 'entity:euphorbia ampanihyensis', 'entity:euphorbia robinsonii', 'entity:euphorbia sprirosticha', 'entity:euphorbia quartziticola', 'entity:euphorbia tulearensis', 'entity:euphorbia capsaintemariensis tulearensis', 'entity:euphorbia francoisii', 'entity:euphorbia parvicyathophora', 'entity:euphorbia handiensis', 'entity:euphorbia lambii', 'entity:euphorbia bourgeana', 'entity:euphorbia stygiana', 'entity:e decaryi', 'entity:e ampanihyensis', 'entity:e robinsonii', 'entity:e sprirosticha', 'entity:e quartziticola', 'entity:e tulearensis', 'entity:e capsaintemariensis tulearensis', 'entity:e francoisii', 'entity:e parvicyathophora', 'entity:e handiensis', 'entity:e lambii', 'entity:e bourgeana', 'entity:e stygiana', 'entity:cardon de jandia', 'entity:tabaiba amarilla de tenerife', 'entity:stangeria eriopus', 'entity:lomaria eriopus', 'entity:stangeria paradoxa', 'entity:stangeria katzeri', 'entity:stangeria schizodon', 'entity:natal grass cycad', 'entity:encephalartos natalensis', 'entity:encephalartos ferox', 'entity:encephalartos ghellinckii', 'entity:drakensberg cycad', 'entity:encephalartos ngoyanus', 'entity:encephalartos senticosus', 'entity:lebombo cycad', 'entity:jozini cycad' ],
	#		'entity_freq_range' : None,
	#		},
	#	'avoid' : {
	#		'entity' : [ 'entity:rubbish_name' ],
	#		# if entity freq is > max or < min then its blacklisted
	#		'entity_freq_range' : { 'max' : 100, 'min' : 30 },
	#		},
	#	}

	#
	# match
	#

	nMaxFreq = None
	nMinFreq = None
	if filter_spec['match']['entity_freq_range'] != None :
		nMaxFreq = filter_spec['match']['entity_freq_range']['max']
		nMinFreq = filter_spec['match']['entity_freq_range']['min']

	# no pattern?
	if (nMaxFreq == None) and (nMinFreq == None) and (filter_spec['match']['entity'] == None) :
		# no pattern so return no matches
		return []

	# if we have an entity pattern then make a set of matches that match this, otherwise default to all entities
	setMatch = set([])
	if filter_spec['match']['entity'] != None :
		for strEntityPattern in filter_spec['match']['entity'] :
			for strEntity in entity_index :
				strEntityToMatch = strEntity
				strPatternToMatch = strEntityPattern

				# if needed strip entity prefix from entity match
				if strEntityPattern.startswith('?:') :
					if ':' in strEntityToMatch :
						strEntityToMatch = strEntityToMatch[ strEntityToMatch.index(':') + 2 : ]
						strPatternToMatch = strEntityPattern[3:]

				# now match
				if strPatternToMatch.endswith('*') :
					if strEntityToMatch.startswith( strPatternToMatch[:-1] ) :

						nConnections = 0
						for strEntityLink in entity_index[strEntity] :
							nConnections = nConnections + entity_index[strEntity][strEntityLink]

						bBad = False
						if (nMaxFreq != None) and (nConnections > nMaxFreq) :
							bBad = True
						if (nMinFreq != None) and (nConnections < nMinFreq) :
							bBad = True

						if bBad == False :
							setMatch.add( strEntity )
				elif strPatternToMatch.startswith('*') :
					if strEntityToMatch.endswith( strPatternToMatch[1:] ) :

						nConnections = 0
						for strEntityLink in entity_index[strEntity] :
							nConnections = nConnections + entity_index[strEntity][strEntityLink]

						bBad = False
						if (nMaxFreq != None) and (nConnections > nMaxFreq) :
							bBad = True
						if (nMinFreq != None) and (nConnections < nMinFreq) :
							bBad = True

						if bBad == False :
							setMatch.add( strEntity )
				else :
					if strPatternToMatch == strEntityToMatch :

						nConnections = 0
						for strEntityLink in entity_index[strEntity] :
							nConnections = nConnections + entity_index[strEntity][strEntityLink]

						bBad = False
						if (nMaxFreq != None) and (nConnections > nMaxFreq) :
							bBad = True
						if (nMinFreq != None) and (nConnections < nMinFreq) :
							bBad = True

						if bBad == False :
							setMatch.add( strEntity )
	else :
		for strEntity in entity_index :
			setMatch.add( strEntity )

	# if we have an freq pattern then applt this to all match candidates
	if (nMaxFreq != None) or (nMinFreq != None) :
		listToCheck = list( setMatch )
		for strEntity in listToCheck :

			nConnections = 0
			for strEntityLink in entity_index[strEntity] :
				nConnections = nConnections + entity_index[strEntity][strEntityLink]

			bBad = False
			if (nMaxFreq != None) and (nConnections > nMaxFreq) :
				bBad = True
			if (nMinFreq != None) and (nConnections < nMinFreq) :
				bBad = True

			if bBad == False :
				setMatch.remove( strEntity )

	#
	# avoid
	#

	nMaxFreq = None
	nMinFreq = None
	if filter_spec['avoid']['entity_freq_range'] != None :
		nMaxFreq = filter_spec['avoid']['entity_freq_range']['max']
		nMinFreq = filter_spec['avoid']['entity_freq_range']['min']

	# get banned matches
	setBanned = set([])
	if filter_spec['avoid']['entity'] != None :
		for strEntityPattern in filter_spec['avoid']['entity'] :
			if strEntityPattern.endswith('*') :
				for strEntity in setMatch :
					if strEntity.startswith( strEntityPattern[:-1] ) :

						nConnections = 0
						for strEntityLink in entity_index[strEntity] :
							nConnections = nConnections + entity_index[strEntity][strEntityLink]

						bBad = False
						if (nMaxFreq != None) and (nConnections > nMaxFreq) :
							bBad = True
						if (nMinFreq != None) and (nConnections < nMinFreq) :
							bBad = True

						if bBad == False :
							setBanned.add( strEntity )
			else :
				if strEntityPattern in setMatch:
					nConnections = 0
					for strEntityLink in entity_index[strEntityPattern] :
						nConnections = nConnections + entity_index[strEntityPattern][strEntityLink]

					bBad = False
					if (nMaxFreq != None) and (nConnections > nMaxFreq) :
						bBad = True
					if (nMinFreq != None) and (nConnections < nMinFreq) :
						bBad = True

					if bBad == False :
						setBanned.add( strEntityPattern )

	elif (nMaxFreq != None) or (nMinFreq != None) :
		# no entity name pattern to match but we do have a freq range (so match any entity that has a freq in this range)
		for strEntity in setMatch :

			nConnections = 0
			for strEntityLink in entity_index[strEntity] :
				nConnections = nConnections + entity_index[strEntity][strEntityLink]

			bBad = False
			if (nMaxFreq != None) and (nConnections > nMaxFreq) :
				bBad = True
			if (nMinFreq != None) and (nConnections < nMinFreq) :
				bBad = True

			if bBad == False :
				setBanned.add( strEntity )

	#
	# remove banned matches
	#

	for strEntity in setBanned :
		setMatch.remove( strEntity )
	
	# all done
	return list( setMatch )

def cluster_index( entity_index = None, list_root_nodes = None, dict_config = {} ):
	"""
	cluster entity index according to a cluster spec. any matching entities will be deleted, and index connections replaced to point to cluster.
	root nodes cannot be included in a cluster.

	:param dict entity_index: index created by load_data_graph()
	:param list list_root_nodes: list of root nodes
	:param dict dict_config: config object
	:return: new index of entities after cluster is created
	:rtype: dict
	"""

	# copy index
	dictEntityIndex = copy.deepcopy( entity_index )

	# compile a list of entities belonging to each cluster
	for strClusterID in dict_config['cluster_spec'] :
		# get all matching entities
		listClusterEntities = entity_lookup_using_filter(
					entity_index = dictEntityIndex,
					filter_spec = dict_config['cluster_spec'][strClusterID]
					)

		dict_config['logger'].info( strClusterID + ' # ' + str(len(listClusterEntities)) + ' entities' )

		# remove any root nodes
		for strRootNode in list_root_nodes :
			if strRootNode in listClusterEntities :
				listClusterEntities.remove( strRootNode )

		# add cluster to index
		dictEntityIndex[ strClusterID ] = {}

		# replace any reference to a cluster entity with a reference to the new cluster (removing the original entity reference)
		for strEntity in dictEntityIndex :
			for strClusterEntity in listClusterEntities :
				if strClusterEntity in dictEntityIndex[strEntity] :
					nFreq = dictEntityIndex[strEntity][strClusterEntity]

					if not strClusterID in dictEntityIndex[strEntity] :
						dictEntityIndex[strEntity][strClusterID] = nFreq
					else :
						dictEntityIndex[strEntity][strClusterID] = dictEntityIndex[strEntity][strClusterID] + nFreq
					
					del dictEntityIndex[strEntity][strClusterEntity]

		# copy all connections from cluster entities and add them as connections from cluster
		for strClusterEntity in listClusterEntities :
			for strEntityLinked in dictEntityIndex[strClusterEntity] :
				if not strEntityLinked in listClusterEntities :

					nFreq = dictEntityIndex[strClusterEntity][strEntityLinked]

					if not strEntityLinked in dictEntityIndex[ strClusterID ] :
						dictEntityIndex[ strClusterID ][ strEntityLinked ] = nFreq
					else :
						dictEntityIndex[ strClusterID ][ strEntityLinked ] = dictEntityIndex[ strClusterID ][ strEntityLinked ] + nFreq

		# remove all cluster entities
		for strClusterEntity in listClusterEntities :
			del dictEntityIndex[ strClusterEntity ]

	return dictEntityIndex

def filter_index( entity_index = None, list_root_nodes = None, filter_spec = None, dict_config = {} ):
	"""
	filter the index using the filter defined in dict_config.
	root nodes cannot be filtered out.

	:param dict entity_index: index created by load_data_graph()
	:param list list_root_nodes: list of root nodes
	:param dict filter_spec: filter spec to apply
	:param dict dict_config: config object
	:return: new index of entities after filtering
	:rtype: dict
	"""

	# copy index
	dictEntityIndex = copy.deepcopy( entity_index )

	# get a list of all nodes that match the filter list
	listEntityToFilter = entity_lookup_using_filter(
				entity_index = dictEntityIndex,
				filter_spec = filter_spec,
				dict_config = dict_config
				)

	# remove root nodes from filter set
	for strEntity in list_root_nodes :
		if strEntity in listEntityToFilter :
			listEntityToFilter.remove( strEntity )

	# for each entity to be filtered, remove it and removed any connections to it
	for strEntityToBeRemoved in listEntityToFilter :

		# remove any link to this entity
		for strEntity in dictEntityIndex :
			if strEntityToBeRemoved in dictEntityIndex[strEntity] :
				del dictEntityIndex[strEntity][strEntityToBeRemoved]

		# remove the entity
		del dictEntityIndex[strEntityToBeRemoved]

	# all done
	return dictEntityIndex

def bfs( G, start, entity_index = None, search_depth = None, list_direction = None ):
	"""
	breadth first search of entity index to populate networkx graph object with nodes and edges. graphs start from a root nodes

	:param G: which is the graph
	:param start: root node
	:param dict entity_index: index created by load_data_graph()
	:param search_depth: depth of graph to build
	:param list_direction: direction of graph walk
	"""
	listEBunch = []
	listVisited = []
	queueNodes = [(start, 0)]

	while len(queueNodes) > 0 :

		tupleNode = queueNodes.pop(0)
		strNode = tupleNode[0]
		nLevel = tupleNode[1]

		if nLevel < search_depth :

			nIndexBunch = len(listEBunch)

			generate_new_list( entity = strNode, entity_index = entity_index, list_direction = list_direction, ebunch = listEBunch )

			# lookup any new connected nodes and add them to the queue (if they have not been processed already)
			if len(listEBunch) > nIndexBunch :
				for ( strEntity1, strEntity2, nWeight ) in listEBunch[ nIndexBunch : ] :

					if strEntity1 not in listVisited:
						queueNodes.append( (strEntity1, nLevel+1) )
						listVisited.append( strEntity1 )

					if strEntity2 not in listVisited:
						queueNodes.append( (strEntity2, nLevel+1) )
						listVisited.append( strEntity2 )

	# add edges (this will add nodes if they are missing)
	# note: using ebunch is orders of magnitude more efficient way to build a graph in networkx than using many add_edge() calls
	G.add_weighted_edges_from( listEBunch, weight='weight' )


def generate_new_list( entity = None, entity_index = None, list_direction = None, ebunch = None ):
	"""
	internal function called by bfs

	:param entity: entity to process
	:param dict entity_index: index created by load_data_graph()
	:param list_direction: direction of graph walk
	:param ebunch: reference list of edges to be added later
	"""

	if entity in entity_index:
		if 'forward' in list_direction :

			# note all connected entities to process next
			for strEntityLinked in entity_index[ entity ] :
				ebunch.append( ( entity, strEntityLinked, 1 ) )

		if 'backward' in list_direction :

			# note all entities that have this entity in its connection list
			for strEntityLinked in entity_index :
				if entity in entity_index[strEntityLinked] :
					ebunch.append( ( entity, strEntityLinked, 1 ) )

def aggregate_nodes_with_same_base( G, entity_index = None, root_node_list = None, filter_post_freq = None ):
	"""
	aggregate nodes with the same name but different posts (e.g. mention_post1 + mention_post2 -> mention).
	this is not done after the graph is built, so we preserve the post/thread conversation connections. otherwise we would confusingly aggregate entities with same name from any post context.
	root nodes cannot be aggregated.

	:param G: which is the graph
	:param dict entity_index: index created by load_data_graph()
	:param root_node_list: list of root nodes
	:param filter_post_freq: minimum post frequency allowed (can be None to disable post freq filtering)
	"""

	# get base name of all nodes in the graph
	setBaseNames = set([])
	for strNode in G :
		strBase = strNode
		if '@@@' in strNode :
			strBase = strNode.split('@@@')[0]
		setBaseNames.add( strBase )

	# get a list of node occurances of each base name
	for strBase in setBaseNames :

		listMatch = []
		for strNode in G :
			if strNode.startswith( strBase ) == True :
				# make sure its either the exact base OR has a _ suffix (so its not a coincidental match where base happens to ba esame as another entity)
				if (strNode == strBase) or (strNode[len(strBase):].startswith( '@@@' ) ) :
					listMatch.append( strNode )

		if len(listMatch) == 0 :
			raise Exception('no node with base')

		strNodeToKeep = listMatch[0]
		listNodesToRemove = listMatch[1:]

		# if this post node is below threshold then remove it entirely
		if filter_post_freq != None :
			if strNodeToKeep.startswith('posts[') :
				if len(listMatch) < filter_post_freq :
					# remove all post nodes if they represent too few posts (below threshold)
					for strNodeToRemove in listMatch :
						G.remove_node( strNodeToRemove )
					continue

		# only 1 occurance then there is no aggregation to perform
		if len(listMatch) == 1 :
			#print( 'Ignoring node = ' + repr(listMatch[0]) )
			continue

		#print( 'Matches = ' + repr(listMatch) )

		# loop on all matches 1..N and remove them, first relocating any edges (aggregating them if they are now duplicates)
		for strNodeToRemove in listNodesToRemove :

			# loop on all edges from node to remove
			dictEdges = G[strNodeToRemove]
			for strChildNode in dictEdges :
				nWeight = dictEdges[strChildNode]['weight']

				# update edges of node to keep
				dictEdgesUpdated = G[strNodeToKeep]
				if strChildNode in dictEdgesUpdated :
					dictEdgesUpdated[strChildNode]['weight'] += nWeight
				else :
					G.add_edge( strNodeToKeep, strChildNode, weight = nWeight )

			# loop on all nodes which have an edge linked to node to remove
			for strNodeParent in G :
				if strNodeToRemove in G[strNodeParent] :
					dictEdges = G[strNodeParent]
					nWeight = dictEdges[strNodeToRemove]['weight']

					# update edges of parent node to link it to node to keep
					dictEdgesUpdated = G[strNodeParent]
					if strNodeToKeep in dictEdgesUpdated :
						dictEdgesUpdated[strNodeToKeep]['weight'] += nWeight
					else :
						G.add_edge( strNodeParent, strNodeToKeep, weight = nWeight )

			# remove the node (this will also remove any edges it has)
			G.remove_node( strNodeToRemove )


	# remove any nodes that are now widows
	queueNodes = []
	for strNode in G :
		queueNodes.append( strNode )
	for strNode in queueNodes :
		dictEdges = G[strNode]
		if len(dictEdges) == 0 :
			G.remove_node( strNode )