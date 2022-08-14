# Homework Rock Scissors Paper Game by Python
# Created by Natchapol Laowiwatkasem - 2022-07-22
# Section 1 - https://colab.research.google.com/drive/1RLESETrV9z8uRD46f4nph1r0RLMIXBYc?usp=sharing

import random
from tabulate import tabulate

# define rock scissors paper game function
def rsp_game():
    # define objects to record winning statistics
    win = 0
    lose = 0
    draw = 0

    # using while loop for playing game
    while True:
        # get input from user (3 choices and exit choice for quit the game)
        user_input = input("Type your choice: rock/ scissors/ paper/ exit == ").lower()
        # random rock or scissors or paper for bot hand
        bot_random = random.choice(["rock", "scissors", "paper"])
        # table object for display result of chosen choice
        table = [["user", user_input], ["bot", bot_random]]

        # create conditions of game
        # 1. draw condition
        if user_input == bot_random:
            # print table of result
            print(tabulate(table, headers = [" ", "DRAW!"], tablefmt = "pretty"))
            # record draw count + 1
            draw += 1
        # 2. win condition
        elif (user_input == "rock" and bot_random == "scissors") \
            or (user_input == "scissors" and bot_random == "paper") \
            or (user_input == "paper" and bot_random == "rock"):
            # print table of result
            print(tabulate(table, headers = [" ", "WIN!"], tablefmt = "pretty"))
            # record win count + 1
            win += 1
        # 3. lose condition
        elif (user_input == "rock" and bot_random == "paper") \
            or (user_input == "scissors" and bot_random == "rock") \
            or (user_input == "paper" and bot_random == "scissors"):
            # print table of result
            print(tabulate(table, headers = [" ", "LOSE!"], tablefmt = "pretty"))
            # record lose count + 1
            lose += 1
        # 4. exit game condition
        elif user_input == "exit":
            # print result table of winning statistics
            print("See you soon!")
            table_result = [["win", win], ["lose", lose], ["draw", draw]]
            print(tabulate(table_result, headers = ["stats", "count"], tablefmt="pretty"))
            # break the loop
            break
        # 5. if player does not type 4 choices
        else:
            # print try again and back to select choice
            print("Please type again. (rock/ scissors/ paper/ exit)")
