# -*- coding: utf-8 -*-
import getopt
import math
import sys
import random
import cplex
import numpy as np
import time


def ReadInput():
    global input_file_path
    global entities
    global connections
    global down_sen
    global agg_values
    
    # define the variables to store info from the input information
    # sensitivity for each entity
    ent_sense_dic = {}
    id_dic = {}
    id_num = 0
    down_sen = 0
    entities = []
    connections = []
    agg_values = []
    input_file = open(input_file_path,'r')
    for line in input_file.readlines():
        elements = line.split()
        connection = []
        # First value is the value for aggregation
        agg_value = float(elements[0])
        # For each entity relate to that join result
        for element in elements[1:]:
            element = int(element)
            # Re-order the IDs
            if element in id_dic.keys():
                element = id_dic[element]
            else:
                entities.append(id_num)
                id_dic[element] = id_num
                element = id_num
                id_num+=1
			#Update the values of DS and entities for each element
            if element in  ent_sense_dic.keys():
                 ent_sense_dic[element]+=agg_value
            else:
                 ent_sense_dic[element]=agg_value
            if down_sen<= ent_sense_dic[element]:
                down_sen = ent_sense_dic[element];				
            connection.append(element)
        connections.append(connection)
        agg_values.append(agg_value)
      
def LapNoise():
	noise = math.log(1 / (1 - random.uniform(0, 1)))
	random_toss = random.uniform(0, 1)
	if random_toss > 0.5:
		return noise
	else:
		return -noise

def LPSolver(tau):
    global entities
    global connections
    num_constraints = len(entities)
    num_variables = len(connections)
    
    # create the object of linear programming
    cpx = cplex.Cplex()
    cpx.objective.set_sense(cpx.objective.sense.maximize)

    # create variables
    obj = np.ones(num_variables)
    ub = np.zeros(num_variables)
    for i in range(num_variables):
        ub[i]=agg_values[i]
    cpx.variables.add(obj=obj, ub=ub)
    
    # setup the RHS
    rhs = np.ones(num_constraints)*tau
    senses = "L" * num_constraints
    cpx.linear_constraints.add(rhs=rhs, senses=senses)
    
    # create coefficients
    cols = []
    rows = []
    vals = []
    
    for i in range(num_variables):
        for j in connections[i]:
            cols.append(i)
            rows.append(j)
            vals.append(1)
    cpx.linear_constraints.set_coefficients(zip(rows, cols, vals))
    cpx.set_log_stream(None)
    cpx.set_error_stream(None)
    cpx.set_warning_stream(None)
    cpx.set_results_stream(None)
    cpx.solve()
    return cpx.solution.get_objective_value()    

def RunAlgorithm():
    global global_sensitivity
    global connections
    global down_sen
    global tilde_Q_tau
    global Q_tau
    global hat_Q_tau
    global epsilon
    global beta
    global real_query_result

    base = 2.0
    # initilize the max interation
    max_i = int(math.log(global_sensitivity, base))
    if max_i <= 1:
        max_i += 1
    real_query_result = len(connections)

    # Used to store the results
    Q_tau = {}
    tilde_Q_tau = {}
    hat_Q_tau = {}

    # initialize tau's
    for i in range(1, max_i + 1):
        Q_tau[i] = 0
        tilde_Q_tau[i] = 0
        hat_Q_tau[i] = 0

    for i in range(1, max_i + 1):
        ThresholdRunAlgorithm(epsilon, beta, base, max_i, down_sen, real_query_result, [i])

    max_ind = 1
    max_val = 0
    for i in range(1, max_i + 1):
        if tilde_Q_tau[i] > max_val:
            max_val = tilde_Q_tau[i]
            max_ind = i
    final_res = tilde_Q_tau[max_ind]
    return final_res

def ThresholdRunAlgorithm(epsilon,beta,base, max_i, down_sen, real_query_result, assigned_taus):
    global tilde_Q_tau
    global Q_tau
    global hat_Q_tau
    for i in assigned_taus:
        tau = math.pow(base,i)
        if tau>=down_sens:
            t_res = real_query_result
        else:   
            t_res = LPSolver(tau)
        Q_tau[i] = t_res
        hat_Q_tau[i] = t_res+LapNoise()*tau/epsilon*max_i
        tilde_Q_tau[i] = hat_Q_tau[i]-tau/epsilon*max_i*math.log(max_i/beta,2.9718)

def main(argv):
    global input_file_path
    global epsilon
    global beta
    global global_sensitivity
    global processor_num
    global real_query_result
	
    try:
        opts, args = getopt.getopt(argv,"h:I:e:b:G:p:",["Input=","epsilon=","beta=","GlobalSensitivity="])
    except getopt.GetoptError:
        print("R2TOld.py -I <input file> -e <epsilon(default 0.8)> -b <beta(default 0.1)> -G <global sensitivity(default 1024)>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("R2TOld.py -I <input file> -e <epsilon(default 0.8)> -b <beta(default 0.1)> -G <global sensitivity(default 1024)>")
            sys.exit()
        elif opt in ("-I", "--Input"):
            input_file_path = str(arg)
        elif opt in ("-e","--epsilon"):
            epsilon = float(arg)
        elif opt in ("-b","--beta"):
            beta = float(arg)
        elif opt in ("-G","--GlobalSensitivity"):
            global_sensitivity = float(arg)
    start = time.time()
    ReadInput()
    res = RunAlgorithm()
    end= time.time()
    print("Query Result")
    print(real_query_result)
    print("Noised Result")
    print(res)
    print("Time")
    print(end-start)

if __name__ == "__main__":
	main(sys.argv[1:])