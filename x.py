a=range(1000)
a="ramkumarabcdefghijklmnopqrstuvwxyz"

def gen(a):
    o=()
    for i in a:
        o+=(i,)
        # print(2,len(o),len(a))
        if ((len(o)==10) or (len(o)==len(a))):
            # print(1,len(o),len(a))
            yield o
            o=()
    else: 
        yield o        

for i in gen(a):
    print(i)

# def gen(a):
#     o=()
#     for i in a:
#         if ((len(o)==10) or (len(o)>=len(a)-1)):
#             print(1,len(o),len(a))
#             yield o
#             o=()
#         else:
#             print(2,len(o),len(a))
#             o+=(i,)