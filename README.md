# SparkExample

This Spark sample contain 2 Assignment
1> Assume there is on text file of logs(Assignment 1)
    Log File:
    userId, productId, action
    action can be Browse, Click, AddtoCart.....
    Problem Statement: Need to write a spark mapReduce code to find top 10 most viewed products.
    A product is considered most viewed if the action contains Browse or Clicked. Output should contin count of Crowse and count of click seperately
    in descending order of (browse count and click count)
