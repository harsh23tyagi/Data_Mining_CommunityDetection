from pyspark.context import SparkContext, SparkConf
import sys
import copy
import time

items = None
inputfile = sys.argv[2]  # "sample_data.csv"  # sys.argv[1]
sc = None
filterThreshold = float(sys.argv[1])  # 7
cost_dict = {}
outputFile = sys.argv[3]  # "output_pt_a.txt"
outputFile2 = sys.argv[4]  # "output_pt_b.txt"
t = None
totalEdges = 0.0
strict_totalNodes = []
adjacency_listMain = {}


def initialize():
    global items, inputfile, sc, filterThreshold, t, totalEdges, cost_dict, strict_totalNodes, adjacency_listMain
    t = time.time()
    sc_conf = SparkConf()
    sc_conf.setAppName("Task1")
    sc_conf.setMaster('local[*]')
    # sc_conf.set("spark.driver.bindAddress", "127.0.0.1")
    sc = SparkContext(conf=sc_conf)
    sc.setLogLevel("ERROR")
    csvread = sc.textFile(inputfile)
    columnName = csvread.first().split(',')
    # print(columnName)
    items = csvread.map(lambda line: line.split(",")).filter(
        lambda line: (line) != columnName)

    # Getting user and their business count
    user_business = items.groupByKey().mapValues(set).collect()
    tuple_edge_list = []

    for i in range(0, len(user_business)-1):
        for j in range(i+1, len(user_business)):
            inter = user_business[i][1] & user_business[j][1]
            if len(inter) >= filterThreshold:
                tuple_edge_list.append(
                    (str(user_business[i][0]), str(user_business[j][0])))
                tuple_edge_list.append(
                    (str(user_business[j][0]), str(user_business[i][0])))

    totalEdges = float(len(tuple_edge_list)/2)
    adjacency_list = sc.parallelize(
        tuple_edge_list).groupByKey().mapValues(list).collectAsMap()
    adjacency_listMain = copy.deepcopy(adjacency_list)
    totalNodes = list(adjacency_list.keys())

    # ------------------------Newly added line------------------------
    strict_totalNodes = copy.deepcopy(totalNodes)
    # print(len(totalNodes))

    # ----------------------Part 1---------------------
    bfs(totalNodes, adjacency_list)
    print("Writing Betweenness to File....")

    # Converting into sorted List Initial Betweenness
    list_val = list(cost_dict.items())

    list_val.sort(key=lambda x: (-x[1], x[0]))
    writeToFile(list_val)
    totalNodes = copy.deepcopy(strict_totalNodes)
    # print(len(totalNodes))
    # ----------------------Part 2----------------------
    print("Creating Partitions....")
    create_components(list_val, adjacency_listMain, totalNodes, totalEdges)
    # ---------------------EoC---------------------------

    print("Duration: "+str(time.time()-t))


def bfs(totalNodes, adj_list):
    global cost_dict
    cost_dict = {}
    bfs_queue = []
    dict_info = {}
    count = 1
    # print(adj_list['I_3AUfSj6nXecO1xivgWLQ'])
    # print("Calculating betweenness.....")
    # count = 1
    while(len(totalNodes) != 0):
        root = totalNodes.pop(0)
        NewbfsOnOne(root, adj_list)
        # count += 1
    # print(str(count))

    for k, v in cost_dict.items():
        cost_dict[k] = v/2

    # list_val = list(cost_dict.items())

    # list_val.sort(key=lambda x: (-x[1], x[0]))
    # print(list_val)
    # print(len(cost_dict))


def writeToFile(list_val):
    global outputFile

    with open(outputFile, 'w') as writeFile:
        # writer = csv.writer(writeFile)
        # strign = "('userid1', 'userid2')"
        # writeFile.write(strign+", betweenness\n")
        for each in list_val:
            strign = "('"+each[0][0]+"', '"+each[0][1]+"'), " + str(each[1])
            writeFile.write(strign+"\n")


# ------------------New Bfs------------------
def NewbfsOnOne(rt, adj_list):
    global cost_dict
    root = rt  # totalNodes.pop(0)
    queue = []
    queue.append(root)

    parent = {}
    parent[root] = []
    visited = {}
    level = {}
    visited[root] = 1
    level[root] = 0
    level_nodes = {}
    level_nodes[0] = [root]
    maximum = 0
    label = {}
    label[root] = 1.0
    while(len(queue) != 0):
        current = queue.pop(0)
        if(not adj_list.get(current)):
            continue
        for each in adj_list[current]:
            if(visited.get(each)):
                if(level.get(each) == level.get(current)+1):  # >= or == check this
                    parent[each].append(current)
                # new cd
                    label[each] += label[current]
            else:
                if(parent.get(each)):
                    parent[each].extend(current)
                else:
                    parent[each] = [current]
                # new cd
                label[each] = label[current]

                level[each] = level[current]+1
                visited[each] = 1
                queue.append(each)
                if (level_nodes.get((level[current]+1))):
                    level_nodes[(level[current]+1)].append(each)
                else:
                    level_nodes[(level[current]+1)] = [each]
                if((level[current]+1) > maximum):

                    maximum = (level[current]+1)

    # commented
    # for key, value in parent.items():
    #     label[key] = len(value)

    # print(label)
    # print(parent)
    # print(level)
    # print(level_nodes)
    # print(maximum)
    # print('--------------------------------------')
    k = maximum
    cost_node = {}
    while (k > 0):
        current_list = level_nodes[k]
        for each in current_list:
            parent_list = parent[each]
            cost_each = 1
            if(cost_node.get(each)):
                cost_each = cost_node.get(each)

            else:
                cost_node[each] = 1

            # div = float(len(parent_list))
            total = 0
            for par in parent_list:
                total += label[par]

            if total == 0:
                tup = tuple(sorted([root, each]))
                if(cost_dict.get(tup)):
                        # change this here to add the betweenness
                    cost_dict[tup] = cost_dict[tup]+float((cost_each))
                else:
                    cost_dict[tup] = float((cost_each))
                continue
                # root
            for par in parent_list:
                div = float(float(label[par])/float(total))
                # print(str(div)+"\n\n")
                if(cost_node.get(par)):
                    cost_node[par] = float(
                        cost_node.get(par)+float((cost_each*div)))
                else:
                    cost_node[par] = float(1+float((cost_each*div)))

                tup = tuple(sorted([par, each]))
                # print(tup)
                if(cost_dict.get(tup)):
                    # change this here to add the betweenness
                    cost_dict[tup] = cost_dict[tup]+float((cost_each*div))
                    # cost_dict[tup] = cost_dict[tup]+cost_node[par]
                else:
                    cost_dict[tup] = float((cost_each*div))

        k -= 1
    # print(cost_node)
    # print(root)
    # print(cost_dict)
    # print("------------------------")
    # print("")


# --------------------New Bfs ends-----------

# ================================================================ Part 2 begins# ================================================================


def create_components(list_val, mainAdj, totalNodes, totalEdges):
    # Calculating Initial Modularity
    global cost_dict

    mod_0 = 0.0

    count = 0
    newModularity = mod_0
    allNodes = copy.copy(totalNodes)
    mainAd = copy.deepcopy(mainAdj)
    maxComp = {}
    # print(list_val)
    # while(mod_0 < newModularity or count == 0):
    while(list_val):
        # print("=============")
        # print(list_val)
        # Remove highest edge
        comp = endingBFS(mainAdj, allNodes)
        # print("Components")
        # print("")
        # print((comp))
        if (count > 0):
            # mod_0 = newModularity
            pass
        else:
            mod_0 = calculateModularity2(
                mainAdj, comp, totalNodes, totalEdges)

        # print(mod_0)
        # print(mainAdj)
        # print(list_val)
        # return

        count += 1

        node_1 = list_val[0][0][0]
        node_2 = list_val[0][0][1]
        (mainAdj[node_1].remove(node_2))
        (mainAdj[node_2].remove(node_1))
        # print(mainAdj)

        newModularity = calculateModularity2(
            mainAdj, comp, totalNodes, totalEdges)
        if(mod_0 < newModularity):
            mod_0 = newModularity
            maxComp = copy.copy(comp)
        # print(newModularity)
        totalNodes = copy.copy(allNodes)
        # print(totalNodes)
        bfs(totalNodes, mainAdj)
        totalNodes = copy.copy(allNodes)
        list_val = []
        list_val = list(cost_dict.items())

        list_val.sort(key=lambda x: (-x[1], x[0]))

        # print("Betweeness")
        # print(list_val)
        # print("Old Modularity: "+str(mod_0))
        # print("New Modularity: "+str(newModularity))
        # print(mainAdj)

        # return
        # print("=============")
    print("Iterations: "+str(count))

    # print(maxComp)
    # print("Final Modularity: "+str((mod_0)))
    print("Total Communities: "+str(len(maxComp)))

    writeToFilePt2(maxComp)
    pass


def calculateModularity(component_adj_list, main_adj_list, totalNodes, totalEdges):
    global adjacency_listMain
    mul = ((1.0/float(2*totalEdges)))
    total = 0.0
    for i in range(0, len(totalNodes)-1):
        adj_nodes = adjacency_listMain[totalNodes[i]]
        # len(adj_nodes)
        a_ij = 0.0
        # float(len(component_adj_list[totalNodes[i]]))
        k_i = float(len(adjacency_listMain[totalNodes[i]]))

        for j in range(i+1, len(totalNodes)):
            if(totalNodes[j] in adj_nodes):
                a_ij = 1.0
            else:
                a_ij = 0.0

            #  float(len(component_adj_list[totalNodes[j]]))
            k_j = float(len(adjacency_listMain[totalNodes[j]]))
            total += a_ij - (float(k_i*k_j)/float(2*totalEdges))
            # print(total)
    modularity_component = float(mul * total)
    # print("Intermediate Modularity: "+str(modularity_component))
    return modularity_component


def endingBFS(adjMatrix, allNodes):
    unvisitedNodes = (copy.copy(allNodes))
    visitedNodes = []
    components = {}
    i = 0
    count = 1
    while(unvisitedNodes):
        root = unvisitedNodes[0]
        queue = []
        queue.append(root)
        visitedNodes = []
        visitedNodes.append(root)
        while(queue):
            rt = queue.pop(0)
            adj_nodes = adjMatrix[rt]
            for each in adj_nodes:
                if(each not in visitedNodes):
                    queue.append(each)
                    visitedNodes.append(each)
        set_inter = list(set(unvisitedNodes) - set(visitedNodes))
        unvisitedNodes = copy.copy(set_inter)
        # print("---Visited Nodes---")
        # print(visitedNodes)
        components[i] = copy.copy(visitedNodes)
        # print("---Component "+str(i)+"---")
        # print(components[i])
        i += 1
        # print("---Unvisited Nodes---")
        # print(unvisitedNodes)
        count += 1
    return components


def calculateModularity2(newAdjMatrix, communities, totalNodes, totalEdges):
    global adjacency_listMain
    mul = ((1.0/float(2*totalEdges)))
    total = 0.0
    modularity_component = 0.0

    for community, nodes in communities.items():
        total = 0.0

        for i in range(0, len(nodes)-1):
            originalNodes = adjacency_listMain[nodes[i]]
            k_i = float(len(adjacency_listMain[nodes[i]]))
            a_ij = 0.0
            for j in range(i+1, len(nodes)):
                k_j = float(len(adjacency_listMain[nodes[j]]))
                if (nodes[j] in originalNodes):
                    a_ij = 1.0
                else:
                    a_ij = 0.0
                total += a_ij - (float(k_i*k_j)/float(2*totalEdges))

        modularity_component += float(total)

    # print("Intermediate Modularity: "+str(modularity_component))
    return float(modularity_component*mul)


def writeToFilePt2(dictionary):
    global outputFile2
    finalList = []
    for key, value in dictionary.items():
        finalList.append(sorted(value))
    finalList.sort(key=lambda x: (len(x), x))
    with open(outputFile2, 'w') as writeFile:
        # strign = "('userid1', 'userid2')"

        for each in finalList:
            writeFile.write("'"+each[0]+"'")
            for (k) in each:

                if(k != each[0]):
                    writeFile.write(", '"+k+"'")
            writeFile.write("\n")
    pass
# ================================================================ Part 2 Ends# ================================================================


def check3():
    global cost_dict, totalEdges, strict_totalNodes

    # adj_list = {}
    # adj_list['A'] = ['B', 'C', 'D']
    # adj_list['B'] = ['A', 'C']
    # adj_list['C'] = ['A', 'B', 'E']
    # adj_list['D'] = ['A', 'E', 'F']
    # adj_list['E'] = ['C', 'D', 'F']
    # adj_list['F'] = ['D', 'E']

    # totalNodes = ['A', 'B', 'C', 'D', 'E', 'F']
    # totalEdges = 8

    adj_list = {}
    adj_list['A'] = ['B', 'E']
    adj_list['B'] = ['A', 'F', 'G']
    adj_list['G'] = ['B', 'D']
    adj_list['D'] = ['G', 'F']
    adj_list['E'] = ['A', 'F']
    adj_list['F'] = ['B', 'D', 'E']

    totalNodes = ['A', 'B', 'G', 'D', 'E', 'F']
    totalEdges = 7

    # adj_list = {}
    # adj_list['A'] = ['B', 'C']
    # adj_list['B'] = ['A', 'C', 'D']
    # adj_list['C'] = ['A', 'B']
    # adj_list['D'] = ['B', 'E', 'F', 'G']
    # adj_list['E'] = ['D', 'F']
    # adj_list['F'] = ['E', 'D', 'G']
    # adj_list['G'] = ['D', 'F']
    # totalNodes = ['E', 'B', 'C', 'D', 'A', 'F', 'G']
    # totalEdges = 9

    adj_list = {}
    adj_list['1'] = ['2', '3']
    adj_list['2'] = ['1', '3']
    adj_list['3'] = ['1', '2', '7']
    adj_list['4'] = ['5', '6']
    adj_list['5'] = ['4', '6']
    adj_list['6'] = ['4', '5', '7']
    adj_list['7'] = ['3', '6', '8']
    adj_list['8'] = ['9', '12', '7']
    adj_list['9'] = ['8', '10', '11']
    adj_list['10'] = ['9', '11']
    adj_list['11'] = ['9', '10']
    adj_list['12'] = ['8', '13', '14']
    adj_list['13'] = ['12', '14']
    adj_list['14'] = ['12', '13']

    totalNodes = ['1', '2', '3', '4', '5', '6',
                  '7', '8', '9', '10', '11', '12', '13', '14']
    totalEdges = 17
    global adjacency_listMain
    adjacency_listMain = adj_list
    # adj_list = {}
    # adj_list['A'] = ['B', 'C']
    # adj_list['B'] = ['A', 'C', 'K']
    # adj_list['C'] = ['A', 'B', 'F', 'K']
    # adj_list['D'] = ['E', 'F']
    # adj_list['E'] = ['D', 'F']
    # adj_list['F'] = ['C', 'D', 'E']
    # adj_list['K'] = ['B', 'C']
    # totalNodes = ['A', 'B', 'C', 'D', 'E', 'F']
    # totalEdges = 9

    count = 1

    strict_totalNodes = copy.copy(totalNodes)

    while(len(totalNodes) != 0):
        root = totalNodes.pop(0)
        # print(root)
        NewbfsOnOne(root, adj_list)
        # print("Completed: "+str(count))
        count += 1
        # if(count == 2):
        #     break

    # root = totalNodes.pop(0)
    for k, v in cost_dict.items():
        cost_dict[k] = v/2

    # print(cost_dict)

    list_val = list(cost_dict.items())

    list_val.sort(key=lambda x: (-x[1], x[0]))
    # print(list_val)
    # print(list_val)
    totalNodes = copy.copy(strict_totalNodes)
    # print(list_val)
    create_components(list_val, adj_list, totalNodes, totalEdges)

    pass


def main():
    initialize()

    # check3()


if __name__ == "__main__":
    print("Started....")
    main()
    print("Completed")
    pass


# -===================== Old===============

# def check():
#     global cost_dict, totalEdges
#     adj_list = {}
#     adj_list['A'] = ['B', 'C']
#     adj_list['B'] = ['A', 'C', 'D']
#     adj_list['C'] = ['A', 'B']
#     adj_list['D'] = ['B', 'E', 'F', 'G']
#     adj_list['E'] = ['D', 'F']
#     adj_list['F'] = ['E', 'D', 'G']
#     adj_list['G'] = ['D', 'F']
#     totalNodes = ['E', 'B', 'C', 'D', 'A', 'F', 'G']
#     count = 1
#     totalEdges = 9
#     strict_totalNodes = copy.copy(totalNodes)

#     while(len(totalNodes) != 0):
#         root = totalNodes.pop(0)
#         # print(root)
#         NewbfsOnOne(root, adj_list)
#         # print("Completed: "+str(count))
#         count += 1
#         # if(count == 2):
#         #     break

#     # root = totalNodes.pop(0)
#     for k, v in cost_dict.items():
#         cost_dict[k] = v/2

#     print(cost_dict)

#     list_val = list(cost_dict.items())

#     list_val.sort(key=lambda x: (-x[1], x[0]))
#     totalNodes = copy.copy(strict_totalNodes)
#     create_components(list_val, adj_list, totalNodes)
#     pass


# def check2():
#     global cost_dict
#     adj_list = {}
#     adj_list['1'] = ['2', '3']
#     adj_list['2'] = ['1', '3']
#     adj_list['3'] = ['1', '2', '7']
#     adj_list['4'] = ['5', '6']
#     adj_list['5'] = ['4', '6']
#     adj_list['6'] = ['4', '5', '7']
#     adj_list['7'] = ['3', '6', '8']
#     adj_list['8'] = ['9', '12', '7']
#     adj_list['9'] = ['8', '10', '11']
#     adj_list['10'] = ['9', '11']
#     adj_list['11'] = ['9', '10']
#     adj_list['12'] = ['8', '13', '14']
#     adj_list['13'] = ['12', '14']
#     adj_list['14'] = ['12', '13']

#     totalNodes = ['1', '2', '3', '4', '5', '6',
#                   '7', '8', '9', '10', '11', '12', '13', '14']
#     count = 1

#     while(len(totalNodes) != 0):
#         root = totalNodes.pop(0)
#         # print(root)
#         NewbfsOnOne(root, adj_list)
#         # print("Completed: "+str(count))
#         count += 1

#     # root = totalNodes.pop(0)
#     for k, v in cost_dict.items():
#         cost_dict[k] = v/2

#     print(cost_dict)
#     pass
