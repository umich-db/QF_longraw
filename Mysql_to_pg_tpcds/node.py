import re
import os
import json
from collections import deque

# from .pattern import extractor_pattern_
# regular expressions
number_pattern_ = r"((\d+(\.\d+)?)|(\d+(\.\d+)?e(\+|\-)+\d+))"
operator_pattern_ = r"[\w\s:.,\-<>]*\S"
auxilary_info_pattern_ = r"\s\w*[\w=:\-\+\* \(\)<>#%\'\.,`]+\S"

extractor_pattern_ = r"->\s?({op})({aux})?\s{{{two}}}(\((cost={num}(\.\.{num})?)\srows={num}\)\s+)?(\(actual time=({num}\.\.{num})\srows={num}\sloops={num}\))".format(
        num=number_pattern_, 
        op=operator_pattern_, 
        aux=auxilary_info_pattern_,
        two=2,
    )

pattern = re.compile(pattern=extractor_pattern_)

def extract_data_from_raw(raw_data):
    info = None 
    match = pattern.search(raw_data)
    if match: # have data
        info = {
            "operator": match.group(1),
            "auxilary_info": match.group(2) if match.group(2) else None,
            "estimate_cost_start": float(match.group(5)) if match.group(5) else None,
            "estimate_cost_end": float(match.group(12)) if match.group(12) else None,
            "estimate_rows": float(match.group(18)) if match.group(18) else None,
            "actual_time_start": float(match.group(26)) if match.group(26) else None,
            "actual_time_end": float(match.group(32)) if match.group(32) else None,
            "actual_rows": float(match.group(38)) if match.group(38) else None,
            "actual_loops": float(match.group(44)) if match.group(44) else None,
        }
    elif "actual time=" not in raw_data: # don't have data
        try:
            info = {
                "operator": raw_data.split("-> ")[1],
            }
        except Exception as e:
            print(f"error data:: {raw_data}")
            raise e
    elif raw_data.strip() == "":  # Skip empty lines
        return None
    
    if info is None:
        raise ValueError(f"Info is None, raw_data: {raw_data}")
    
    # print(f"info: {info}")
    return info # if info is not None else f"== unmatched == {raw_data}" # for debug

class Node():
    def __init__(self, data=None):
        # raw data and children node list
        self.raw_data = data
        self.children = []
        # extract data if available
        self.extracted_data = extract_data_from_raw(data)
        # str
        self.tree_str = None

    def add_child(self, data: str):
        new_node = Node(data)
        self.children.append(new_node)

    def __str__(self):
        if (self.tree_str is None):
            self.tree_str = self.get_tree_str()
        return self.tree_str

    def get_tree_str(self):
        # level traversal (Breadth-First Traversal)
        queue = deque()
        res = ""
        queue.append(self)
        cnt = 0
        while (len(queue) != 0):
            next_node = queue[0]; queue.popleft()
            for child in next_node.children:
                queue.append(child)
            res += f"{cnt}: {next_node.extracted_data}\n"
            cnt += 1
        return res

    def to_json(self):
        res = {}
        res["data"] = self.extracted_data
        res["children"] = [child.to_json() for child in self.children]
        return res 