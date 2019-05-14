import boundaries
import osmnx as ox
import networkx as nx
import pickle
import threading
from multiprocessing.pool import ThreadPool


def get_path_time(G, route, result, j): 

    nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)
    pairs = []
    times = []

    for i in range(len(route) - 1):
        pair = (route[i], route[i+1])
        time = float(edges_proj.loc[(edges_proj['u']==pair[0]) & (edges_proj['v']==pair[1]), 'time'])
        pairs.append(pair)
        times.append(time)

    result[j] = (pairs, times)


def get_route(G, curr_loc, dest_loc):
    orig_node = ox.get_nearest_node(G, curr_loc, method='euclidean')
    target_node = ox.get_nearest_node(G, dest_loc, method='euclidean')
    route = nx.shortest_path(G, source=orig_node, target=target_node, weight='time')

    thread_size = len(route) // 4
    threads = []
    threads_list = [None] * 4

    for i in range(4):
        if i == 0:
            route_i = route[:(i + 1) * thread_size + 1]
        elif i < 3:
            route_i = route[i * thread_size - 1:(i + 1) * thread_size + 1]
        else:
            route_i = route[i * thread_size:]
        t_i = threading.Thread(target=get_path_time, args=(G, route_i, threads_list, i)) 
        threads.append(t_i)

    for i in range(4):
        threads[i].start()
    for i in range(4):
        threads[i].join()
    print('Results: ', threads_list)
    pairs = []
    times = []

    for i in range(4):
        pairs_i, times_i = threads_list[i]
        pairs.extend(pairs_i)
        times.extend(times_i)

    print(pairs, times)
    return pairs, times



