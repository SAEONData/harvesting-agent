#!/usr/bin/env python
import argparse
import sys

from agent.agent import Agent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invoke the Agent.", add_help=False)

    parser.add_argument('-h', metavar='HARVESTER_UID', required=True,
                        help="Harvester UID; this value is also used as both the Datasource UID and Repository UID")

    parser.add_argument('-r', metavar='REPOSITORY_URL', required=True,
                        help="URL of the metadata repository")

    parser.add_argument('-u', metavar='USERNAME', required=True,
                        help="Repository username")

    parser.add_argument('-p', metavar='PASSWORD', required=True,
                        help="Repository password")

    parser.add_argument('-i', metavar='INSTITUTION', required=True,
                        help="Institution that owns the repository")

    if len(sys.argv) == 1:
        parser.print_help()
        exit()

    args = parser.parse_args()

    success, message = Agent.invoke_harvester(args.h, args.h, args.h, args.r, args.u, args.p, args.i)
    print("--- Agent.invoke result ---")
    print("success:", success)
    print("message:", message)
    print("---------------------------")
