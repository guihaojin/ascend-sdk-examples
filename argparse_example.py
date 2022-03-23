import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--do-something", default=False, action="store_true")
arguments = parser.parse_args()
if arguments.do_something:
     print("Do something")
else:
     print("Don't do something")
print(f"Check that arguments.do_something={arguments.do_something} is always a boolean value.")
