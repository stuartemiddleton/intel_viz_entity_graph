# intel_viz_entity_graph
Named Entity (NE) based directed graph visualization for intelligence reports

Our graph visualization aims to generate small directed graphs of connected named entities (i.e. people, locations, species and organisations) with the target suspect as the root node. This automates to some degree the approach used in criminological
analysis, where users are first identified and then posts analysed to see who is connected and what behaviours are being exhibited. Entity types are similar to the UK law enforcement and Home Office standard POLE format (People, Object, Location and Events).

The NE directed graph model has hyper-parameters values for NE filters and the depth of the graph. These can be found in the configuration section.

The algorithm takes a set of forum posts (relevant or not) which have had their text sentences labeled using the [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/) toolkit. Mentioned entities, authors and post/thread connections are indexed, filtered and a directed graph created using a target suspect entity as a root node and a breadth first directed graph walk. Entity disambiguation is not performed, with entity names instead suffixed by their post identifier to provide a conversational context prior to aggregation for the visual graph display. This ensures entities within a common posted context are aggregated into a single graph node, whilst entities outside this context appear differentiated as separate nodes.

The breadth first directed graph walk is then performed at a configurable depth (usually 2) in the forward (e.g. thread A contains post B, post B mentions entity C), backward (e.g. entity C was mentioned by post B) or both directions.

Once a target suspect’s graph is generated it is visualized using a matplotlib and networkx visualization. Optionally graphs can be pseudonymized with entity names hashed, which is useful for publication of intelligence graph examples. To make it easier for a criminologist to process 100’s of connected entities on a graph we colour code nodes by entity type. The visualization is interactive, and can be zoomed in and panned around to explore dense data more easily

This work was supported by the Economic and Social Research Council ([FloraGuard project](http://floraguard.org/), ES/R003254/1) and UK Defence and Security Accelerator, a part of the Ministry of Defence ([CYShadowWatch project](https://www.ecs.soton.ac.uk/research/projects/1019), ACC2005442).

# Scientific [publications](https://www.ecs.soton.ac.uk/people/sem03#publications)

```
Stuart E. Middleton,. Anita Lavorgna, Geoff Neumann and David Whitehead. 2020. Information Extraction from the Long Tail: A Socio-Technical AI Approach for Criminology Investigations into the Online Illegal Plant Trade. In Proceedings of ACM Web Science conference (WebSci 2020). ACM, July 6–10, 2020, Southampton, United Kingdom. 4 pages. https://doi.org/10.1145/3394332.3402838
```

# Pre-requisites (earlier versions may be suitable but are untested)

python >= 3.7, matplotlib >= 3.1, networkx >= 2.3

# Usage

```
py .\intel_viz.py <config file> <data graph file>

e.g.

py .\intel_viz.py .\example.ini .\example_data_graph.json
```

# Configuration of visualization

The configuration is contained in an INI file whose location is passed as a command line parameter. Hyperparameters described below.

```
search_depth = graph depth of connection to display e.g. 2
list_direction = list of allowed directions of graph walk e.g. ['forward','backward']
layout_name = networkx layout type e.g. spring, random, spectral or shell
max_nodes = limit for number of nodes in visual graphs to avoid long render times e.g. 500
filter_post_freq = optional minimum post/thread frequency count for nodes (can be None) e.g. None
colour_map = dict of node category and node colour
entity_prefix_map = dict of entity prefixes to identify node category
list_pseudonymization = list of entity types that should be pseudonymized e.g. []
```

Within the configuration INI file there are entity pattern specs to allow selection of (a) graph root nodes (b) nodes to belong to a cluster (c) nodes to allow in the graph. These all use the entity pattern spec structure described below. For (a) and (b) matching nodes will be included in the root or cluster node list. For (c) matching nodes will be included in a filter list and removed from the graph.

```
{
	# positive entity pattern to match a set of nodes
	'match' : {
			# list of entity prefixes in format of <type>:<name>.
			# <type> can be '?' to allow any type. <type> and <name> can include a wildcard '*' at the start or end of a partial string to be matched.
			'entity' : [ 'NER-*' ],

			# min and max entity connection freq within entity index. note this is the global connection freq before the target node graph walk.
			# If entity freq is > max or < min then pattern is not matched. Default is None.
			'entity_freq_range' : { 'max' : 100, 'min' : 30 },
		},

	# negative entity pattern to ensure some nodes are never matched
	'avoid' : {
			# list of entity prefixes in format of <type>:<name>.
			# <type> can be '?' to allow any type. <type> and <name> can include a wildcard '*' at the start or end of a partial string to be matched.
			'entity' : [ 'NER-PERSON:*', 'NER-PLANT:*', 'NER-LOCATION:*', 'NER-CITY:*', 'NER-STATE_OR_PROVINCE:*', 'NER-COUNTRY:*', 'NER-NATIONALITY:*', 'NER-ORGANIZATION:*'],

			# min and max entity connection freq within entity index. note this is the global connection freq before the target node graph walk.
			# If entity freq is > max or < min then pattern is not matched. Default is None.
			'entity_freq_range' : None,
		},
}
```

# Data graph JSON structure

The intelligence graph visualization expects a data graph in the following format. This will usually be programmatically generated from a combination of a web crawler, parser and named entity tagger such as Stanford CoreNLP.

```
{
  <website>_thread_<thread_id>_post_<post_id>: {
    "author": <author_name>,
    "page_url": <post_uri>,
    <sentence_index>: [
      {
        "entity": [
          <NER-label>:<phrase>,
          <NER-label>:<phrase>,
          ...
        ]
      }
    ],
    <sentence_index>: [
      {
        "entity": [
          <NER-label>:<phrase>,
          <NER-label>:<phrase>,
          ...
        ]
      }
    ],
    ...
  },
  <website>_thread_<thread_id>_post_<post_id>: {
    "author": <author_name>,
    "page_url": <post_uri>,
    <sentence_index>: [ ... ],
    ...
  }
}

The post identifier uses the naming convention of "<website>_thread_<thread_id>_post_<post_id>". The thread and post identifier will be parsed from this name pattern and used to provide conversation post/thread nodes in the final visualization.

The <sentence_index> is a global index used to tie entities to a specific conversational context. This avoids graphs connecting named entity mentions using the same term in an unrelated conversational context.

The <NER-label> will be generated by the NER tagger. For Stanford CoreNLP named entity tags include NER-PERSON, NER-LOCATION, NER-CITY, NER-STATE_OR_PROVINCE, NER-COUNTRY, NER-NATIONALITY, NER-ORGANIZATION etc.

```
