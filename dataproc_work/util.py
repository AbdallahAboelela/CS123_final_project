'''
Purpose: Util file for MapReduce (mr_trip.py).
'''
import osmnx_code
import networkx as nx

def get_path_time(G, edges_proj, curr_loc, dest_loc):
    '''
    Purpose:
        Collects streets and time it takes to run each streets assuming 
        running at speed limit given shortest path from current location 
        to destination location using Dikstra's Algorithm. 
    Inputs:
        G (geodataframe): geodataframe of New York Street Network
        edges_proj (geodataframe): geodataframe of edges in New York Street 
            Network
        curr_loc (tuple): current location lat/lng
        dest_loc (tuple): destination location lat/lng
    Returns:
        pairs (list): list of nod_id pairs that represent streets
        times (list): list of times it takes for car to run across each 
        streets
    Note:
        We only return if able to find nearest node within 200 meters away 
        from the current or destination location. Since G only contains 
        street network information for New York, if either the current location 
        or destination is not within New York, the function will try to find 
        the nearest node in New York boundary. This could cause errors, so 
        we exclude such trips from our Map Reduce analysis. 
    '''
    orig_node, orig_dist = osmnx_code.get_nearest_node(G, curr_loc, method='euclidean', return_dist=True)
    target_node, target_dist = osmnx_code.get_nearest_node(G, dest_loc, method='euclidean', return_dist=True)
    route = nx.shortest_path(G, source=orig_node, target=target_node, weight='time')

    if orig_dist > 200 or target_dist > 200:
        return False, False
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
    '''
    Purpose:
        Categorizes the time of day of a trip.
    Inputs:
        dt_obj (datetime object): datetime object that represents the time at which 
        a trip began or ended.  
    '''
    if dt_obj.hour >= 6 and dt_obj.hour < 12:
        return "mor"
    
    elif dt_obj.hour >= 12 and dt_obj.hour < 18:
        return "aft"

    elif dt_obj.hour >= 18 and dt_obj.hour < 24:
        return "eve"
    
    else:
        return "nit"