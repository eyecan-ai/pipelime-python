from pipelime.choixe import XConfig

context = {"letta": 10}
a = XConfig({"salvini": {"$for(letta, sasso)": "$index(sasso)_"}})
print(a.process(context=context))
