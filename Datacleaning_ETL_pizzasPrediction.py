#EXERCICE 2 - DATA CLEANING + ETL

#IMPORT MODULES
import numpy as np
import pandas as pd


#IMPORT CSV's AS PD.DATAFRAMES (EXTRACT)
def extract():
    #Description of each field of the following datasets
    data_description = pd.read_csv('data_dictionary.csv')

    #Each order might have more than 1 pizza  (On average, there's ~ 2 pizzas by order)
    unordered_pizzas_details = pd.read_csv('order_details_2016.csv', sep=';')    #descrives number of pizzas ordered by its pizza_id 
    orders_details = pd.read_csv('orders_2016.csv', sep=';')                   #descrives date of each order

    # (pizza_id) descrives pizza type and size
    # (pizza_type_id) descrives just pizza type
    pizzas_ingredients = pd.read_csv('pizza_types.csv', encoding='latin1')    # descrives ingredients contained in each pizza_type_id
    pizza_type_price = pd.read_csv('pizzas.csv')                                 # associates pizza_id with correspondent pizza_type_id and price
    
    return [data_description, unordered_pizzas_details, orders_details, pizzas_ingredients, pizza_type_price]






#DATA CLEANING
def correct_quantities(x):
    replacements = {1:1, '1':1, 'one':1, 'One':1, -1:1, '-1':1,
                    2:2, '2':2, 'two':2, 'Two':2, -2:2, '-2':2,
                    3:3, '3':3, 'three':3, 'Three':3, -3:3, '-3':2,
                    4:4, '4':4, 'four':4, 'Four':4, -4:4, '-4':4,}
    return replacements[x]

def correct_pizza_IDs(string):
    string = string.lower().replace(' ','_').replace('-','_')
    string = string.replace('0','o').replace('3','e').replace('@','a')
    return string

def data_cleaning(unordered_pizzas_details, orders_details, pizzas_ingredients, pizza_type_price):
    #Order dataframe by ID's
    ordered_pizzas_details = unordered_pizzas_details.sort_values(by='order_details_id')
    orders_details = orders_details.sort_values(by='order_id')
    
    #DATASET 1: oder_details
    #column: quantity
    #Replace nulls with 1's in 'quantity' column
    ordered_pizzas_details = ordered_pizzas_details.fillna(value={'quantity': 1})
    #Correct the rest of values in 'quantity' column
    ordered_pizzas_details['quantity'] = ordered_pizzas_details['quantity'].apply(correct_quantities)



    #columna: pizza_id
    #Eliminate rows without ID
    ordered_pizzas_details = ordered_pizzas_details.dropna()
    #Correct the rest of values in 'pizza_id' column
    ordered_pizzas_details['pizza_id'] = ordered_pizzas_details['pizza_id'].apply(correct_pizza_IDs)

    
    
    #assuming {type of pizza ordered by people} and {date} are independent events
    #we don't need to study pizza_id's distribution throughout the year
    
    return [ordered_pizzas_details, orders_details, pizzas_ingredients, pizza_type_price]






#DATA QUALITY
def data_quality(df):
    #obtain number of null or NaN values
    df = df.isnull().sum()
    print(df)
    
    

    


    
#OBTAIN INGREDIENT QUANTITIES (TRANSFORM)

def ponderate_quatity_by_size(args):
    ponderations = {'S':0.75 , 'M':1, 'L':1.25, 'XL':1.5, 'XXL':1.75}
    ponderation = ponderations[args[2]]
    # print('p:',ponderation)
    # print(args[3])
    # print(ponderation*args[3])
    return ponderation*args[4]


def multiply_by(x, factor):
    return x*factor


def transform(ordered_pizzas_details, orders_details, pizzas_ingredients, pizza_type_price):

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
    df_temp = pizza_type_price_quantity.groupby('pizza_type_id').sum()['ponderated_quantities'].to_frame()
    pizzas_ingredients = pizzas_ingredients.merge(df_temp, on='pizza_type_id', how = 'inner')

    #Obtain ponderated quantities of each ingredient
    all_ingredients = dict()
    for index, row in pizzas_ingredients.iterrows():

        row_ingredients = row['ingredients'].replace(',','').split()

        for i in row_ingredients:
            if (i in all_ingredients.keys()):
                all_ingredients[i] += row['ponderated_quantities']
            else:
                all_ingredients[i] = row['ponderated_quantities']

    #Transformation to diccionary to operate more easily
    ingredients_ponderated_quantities = pd.DataFrame.from_dict(all_ingredients, orient='index')
    ingredients_ponderated_quantities.rename({0: 'quantity'}, axis=1, inplace=True)


    #NOW WE CAN STUDY HOW COUD WE MAKE THE OPTIMAL INGREDIENT ADQUIREMENTS
    #Acording to CRITERIA: Weekly adquirements
    # -> We must calculate how much ingredient quantities we sould adquire in order to finish them all by the end of the week

    ingredients_ponderated_quantities_criteria1 = ingredients_ponderated_quantities
    adq_period = 7 #days  #(adquiremet period)
    hole_period = 365  #days

    #we obtain igredient usage per week
    ingredients_ponderated_quantities_criteria1['per_week'] = (ingredients_ponderated_quantities_criteria1['quantity']*(adq_period/hole_period)).round(2)

    return ingredients_ponderated_quantities_criteria1







#EXPORT RESULTS TO CSV (LOAD)
def load(df):
    df = df.reset_index(level=0)
    df.rename({'index': 'pizza_id'}, axis=1, inplace=True)


    register = open('register.csv', 'w')

    #write the fields names
    register.write('ingredient;quantity/week\n')

    for index, row in df.iterrows():
        #print(row)
        register.write( str(row['pizza_id']) + ';' + str(row['quantity']) + '\n') 

    register.close()
    
    
    
    
    

if __name__ == '__main__':
    
    #EXTRACT
    dfs = extract()
    
    #DATA QUALITY
    print('DATA QUALITY')
    print('Number of null values per field:\n')
    for i in dfs:
        data_quality(i)
        print()

    
    #TRANSFORM
    dfs = data_cleaning(dfs[1], dfs[2], dfs[3], dfs[4])
    #print(dfs)
    df = transform(dfs[0], dfs[1], dfs[2], dfs[3])
    
    #lOAD
    load(df)