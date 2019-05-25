import osmnx_code
import networkx as nx

def get_path_time(G, edges_proj, curr_loc, dest_loc):
    orig_node = osmnx_code.get_nearest_node(G, curr_loc, method='euclidean')
    target_node = osmnx_code.get_nearest_node(G, dest_loc, method='euclidean')
    route = nx.shortest_path(G, source=orig_node, target=target_node, weight='time')
    
    #nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)

    pairs = []
    times = []
    for i in range(len(route) - 1):
        pair = (route[i], route[i+1])

        time = edges_proj.loc[(edges_proj['u']==pair[0]) & (edges_proj['v']==pair[1]), 'time']
        
        pairs.append(pair)

        try:
           times.append(float(time))

        except:
           times.append(float(time.iloc[0]))

    return pairs, times


def get_time_of_day(dt_obj):
    if dt_obj.hour >= 6 and dt_obj.hour < 12:
        return "mor"
    
    elif dt_obj.hour >= 12 and dt_obj.hour < 18:
        return "aft"

    elif dt_obj.hour >= 18 and dt_obj.hour < 24:
        return "eve"
    
    else:
        return "nit"