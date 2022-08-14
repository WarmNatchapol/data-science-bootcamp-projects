# Homework Python Object-Oriented Programming (OOP) - ATM Class
# Created by Natchapol Laowiwatkasem - 2022-07-22
# Section 2 in colab - https://colab.research.google.com/drive/1RLESETrV9z8uRD46f4nph1r0RLMIXBYc?usp=sharing

import random
import sys

# define string to class function
def str_to_class(str):
    return getattr(sys.modules[__name__], str)   

# Create ATM class
class Atm:
    # define initialization
    def __init__(self, name, balance):
        self.name = name
        self.balance = balance

    # define deposite method
    def deposit(self):
        # get deposit amount
        user_dep = int(input("Please enter amount: "))
        # add deposit to account
        self.balance += user_dep
        # print result of deposit and balance
        print(f"Deposit successful! You deposit {user_dep} THB.")
        print(f"Your balance: {self.balance} THB")

    # define withdraw method
    def withdraw(self):
        # get withdraw amount
        user_wit = int(input("Please enter amount: "))
        # checking the withdraw amount and balance
        # if withdrawal amount is less than or equal to balance
        if user_wit <= self.balance:
            # remove the withdrawal amount from the account
            self.balance -= user_wit
            # print result of withdrawal and balance
            print(f"Withdraw successful! You withdraw {user_wit} THB.")
            print(f"Your balance: {self.balance} THB")
        # if withdrawal amount is greater than to balance
        else:
            # notify user to enter less amount
            print("Please enter lesser amount.")
    
    # define checking balance method
    def balance_check(self):
        # print balance
        print(f"Your balance: {self.balance} THB")
    
    # define create OTP method
    def otp(self):
        # random 0-9, 4 numbers
        digits = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        otp = [ random.choice(digits) for digit in range(0, 4) ]
        # print OTP
        print("Your OTP is", "".join(map(str, otp)))
    
    # define transfer method
    def transfer(self):
        # get destination account
        des_acc = input("Please enter destination account: ").lower()
        # get amount to transfer
        user_tra = int(input("Please enter amount: "))
        # checking the transfer amount and balance
        # if transfer amount is less than or equal to balance
        if user_tra <= self.balance:
            # remove the transfer amount from the account
            self.balance -= user_tra
            # change destination account string to class
            des_acc_class = str_to_class(des_acc)
            # add the transfer amount to the destination account
            des_acc_class.balance += user_tra
            # print result of transfer and balance
            print("Transfer successful!")
            print(f"You have transferd {user_tra} THB to {des_acc.title()}")
            print(f"Your balance: {self.balance} THB")
        # if transfer amount is greater than to balance
        else:
            # notify user to enter less amount
            print("Please enter lesser amount.")
