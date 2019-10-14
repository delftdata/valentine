import gurobipy as grb

import random

opt_model = grb.Model(name="MIP Model")

n = 10
m = 5
set_I = range(1, n+1)
set_J = range(1, m+1)
c = {(i,j): random.normalvariate(0,1) for i in set_I for j in set_J}
a = {(i,j): random.normalvariate(0,5) for i in set_I for j in set_J}
l = {(i,j): random.randint(0,10) for i in set_I for j in set_J}
u = {(i,j): random.randint(10,20) for i in set_I for j in set_J}
b = {j: random.randint(0,30) for j in set_J}

# if x is Continuous
x_vars = {(i, j): opt_model.addVar(vtype=grb.GRB.CONTINUOUS, lb=l[i,j], ub= u[i,j], name="x_{0}_{1}".format(i,j))
          for i in set_I for j in set_J}
# if x is Binary
# x_vars  = {(i,j):opt_model.addVar(vtype=grb.GRB.BINARY, name="x_{0}_{1}".format(i,j)) for i in set_I for j in set_J}
# if x is Integer
# x_vars  ={(i,j):opt_model.addVar(vtype=grb.GRB.INTEGER, lb=l[i,j], ub= u[i,j], name="x_{0}_{1}".format(i,j))
#           for i in set_I for j in set_J}

# <= constraints
constraints = {j: opt_model.addConstr(lhs=grb.quicksum(a[i,j] * x_vars[i,j] for i in set_I), sense=grb.GRB.LESS_EQUAL,
                                      rhs=b[j], name="constraint_{0}".format(j)) for j in set_J}

# sense=grb.GRB.GREATER_EQUAL,

# sense=grb.GRB.EQUAL,

objective = grb.quicksum(x_vars[i,j] * c[i,j] for i in set_I for j in set_J)

# for maximization
# opt_model.ModelSense = grb.GRB.MAXIMIZE
# for minimization
opt_model.ModelSense = grb.GRB.MINIMIZE
opt_model.setObjective(objective)

opt_model.optimize()

for v in opt_model.getVars():
    print('%s %g' % (v.varName, v.x))
