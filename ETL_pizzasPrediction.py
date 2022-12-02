#EXERCICE 1 - 

#IMPORT MODULES
import numpy as np
import pandas as pd


#IMPORT CSV's AS PD.DATAFRAMES (EXTRACT)
#Description of each field of the following datasets
data_description = pd.read_csv('data_dictionary.csv')

#Each order might have more than 1 pizza  (On average, there's ~ 2 pizzas by order)
ordered_pizzas_details = pd.read_csv('order_details.csv')    #descrives number of pizzas ordered by its pizza_id 
orders_details = pd.read_csv('orders.csv')                   #descrives date of each order

# (pizza_id) descrives pizza type and size
# (pizza_type_id) descrives just pizza type
pizzas_ingredients = pd.read_csv('pizza_types.csv', encoding='latin1')    # descrives ingredients contained in each pizza_type_id
pizza_type_price = pd.read_csv('pizzas.csv')                                 # associates pizza_id with correspondent pizza_type_id and price



#DEF FUNCTIONS
def ponderate_quatity_by_size(args):
    ponderations = {'S':0.75 , 'M':1, 'L':1.25, 'XL':1.5, 'XXL':1.75}
    ponderation = ponderations[args[2]]
    # print('p:',ponderation)
    # print(args[3])
    # print(ponderation*args[3])
    return ponderation*args[4]


def multiply_by(x, factor):
    return x*factor



#OBTAIN INGREDIENT QUANTITIES (TRANSFORM)

#Obtain number of orders of each pizza_id
number_pizzas_ordered_by_ID = ordered_pizzas_details.groupby('pizza_id').sum()['quantity']   #pd.series

#pd.series to pd.dataframe
df_temp = pd.DataFrame(data = [number_pizzas_ordered_by_ID.values], columns = number_pizzas_ordered_by_ID.index).T  
df_temp = df_temp.reset_index(level=0)
df_temp.rename({0: 'quantity'}, axis=1, inplace=True)

#Incorporate quantity (of [pizza_ID pizza] ordered) to pizza_type_price dataframe
pizza_type_price_quantity = pizza_type_price.merge(df_temp, on='pizza_id', how = 'inner')
pizza_type_price_quantity['ponderated_quantities'] = pizza_type_price_quantity.apply(ponderate_quatity_by_size, axis = 1)

#Obtain ponderated quantities of each pizza_type_id  (According to the sizes 'S':0.75, 'M':1, 'L':1.25, 'XL':1.5, 'XXL':1.75   of pizza_id)
df_temp = pizza_type_price_quantity.groupby('pizza_type_id').sum(numeric_only = False)['ponderated_quantities'].to_frame()
pizzas_ingredients = pizzas_ingredients.merge(df_temp, on='pizza_type_id', how = 'inner')

#Obtain ponderated quantities of each ingredient
all_ingredients = dict()
for index, row in pizzas_ingredients.iterrows():
    
    row_ingredients = row['ingredients'].split(',')
    
    for i in row_ingredients:
        i = i.strip()
        if (i in all_ingredients.keys()):
            all_ingredients[i] += row['ponderated_quantities']
        else:
            all_ingredients[i] = row['ponderated_quantities']
            
#Transformation to diccionary to operate more easily
ingredients_ponderated_quantities = pd.DataFrame.from_dict(all_ingredients, orient='index')
ingredients_ponderated_quantities.rename({0: 'quantity'}, axis=1, inplace=True)




# [ingredients_ponderated_quantities] CONTAINS INGREDIENTS USED IN THE HOLE PERIOD (358 days)
# SO IT IS PREPARED FOR LOAD IN CASE OF NECESARY




#NOW WE CAN STUDY HOW COUD WE MAKE THE OPTIMAL INGREDIENT ADQUIREMENTS
#Acording to CRITERIA: Weekly adquirements
# -> We must calculate how much ingredient quantities we sould order in order to finish them all by the end of the week

ingredients_ponderated_quantities_criteria1 = ingredients_ponderated_quantities
adq_period = 7 #days  #(adquiremet period)
hole_period = 365  #days  

#we obtain igredient usage per week
ingredients_ponderated_quantities_criteria1['per_week'] = (ingredients_ponderated_quantities_criteria1['quantity']*(adq_period/hole_period)).round(2)
print('CRITERIA: Weekly adquirements')
print(ingredients_ponderated_quantities_criteria1)
