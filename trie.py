"""
Copyright (C) 2015  Genome Research Ltd.

Author: Irina Colgiu <ic4@sanger.ac.uk>

This program is part of cookie-monster

cookie-monster is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

This file has been created on Sep 25, 2015.
"""

class Trie(object):

    def __init__(self, val):
        self.value = val
        self.children = []

    def insert_child(self, value):
        self.children.append(Trie(value))

    def insert_dfs(self, string):
        crt = self
        for ch in string:


    def has_child(self, val):
        for child in self.children:
            if child.value == val:
                return True
        return False

    def get_child(self, val):
        for child in self.children:
            if child.value == val:
                return child
        return None


    def contains_word(self, char_list):
        crt = self
        for ch in char_list:
            crt = crt.get_child(ch)
            if not crt:
                return False
        if crt.children:
            return False
        return True

    def __str__(self):
        str_children = [str(child) for child in self.children]
        return "Root: " + str(self.value) + " and children: " + str(str_children)

trie = Trie('/')
trie.insert_child('1')
trie.get_child('1').insert('2')
print str(trie)
