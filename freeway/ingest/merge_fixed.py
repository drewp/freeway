"""
merge two fixed-format files

"""
import sys
from freeway.db import MeasFixedFile

firstFullPath, otherFullPath, outFullPath = sys.argv[1:4]

mff = MeasFixedFile("doesntmatter")
mff.merge(firstFullPath, otherFullPath, outFullPath)

