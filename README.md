# SparkExample

This Spark sample contain 2 Assignment

Assignment 1> Assume there is on text file of logs(Assignment 1)
    
    Log File:
    
    userId, productId, action
    
    action can be Browse, Click, AddtoCart.....
    
    Problem Statement: Need to write a spark mapReduce code to find top 10 most viewed products.
    A product is considered most viewed if the action contains Browse or Clicked. 
    Output should contin count of Crowse and count of click seperately
    in descending order of (browse count and click count)

Assignment 2> Assume there is on text file of logs(Assignment 1)
    
    Log File:
    
    userId, productId, category, action
    
    action can be Browse, Click, AddtoCart.....
    
    category would be Electronics,Fashion, Kids, Food, Books
    
    Problem Statement: Need to write a spark mapReduce code to find Users who are interested in kids category but not in food category
    A user is considered not interested if he/she has performedany of these actions(CLick/AddToCart/Purchase)
    
    OTPUT:
    UserId
    
    
