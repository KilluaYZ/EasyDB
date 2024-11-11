# -*- coding:utf-8 -*- 
# B+树的图形化展示代码，
# 原作者：thursdayhawk http://wiki.jikexueyuan.com/project/python-actual-combat/tutorial-11.html
# 修改成用graphviz图形化显示，修改者:littleZhuHui

import os
from random import randint,choice
from bisect import bisect_right,bisect_left
from collections import deque

idSeed = 0

#生成一个全局ID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
def  getGID():
    global idSeed
    idSeed+=1
    return idSeed

class InitError(Exception):
    pass

class ParaError(Exception):
    pass

# 定义键值对
class KeyValue(object):
    __slots__=('key','value')
    def __init__(self,key,value):
        self.key=key
        self.value=value
    def __str__(self):
        return str((self.key,self.value))
    def __lt__(self, _key):
        return self.key < _key.key

# 内部节点
class Bptree_InterNode(object):

    def __init__(self,M):
        if not isinstance(M,int):
            raise InitError('M must be int')
        if M<=3:
            raise InitError('M must be greater then 3')
        else:
            self.__M=M
            self.childList=[]
            self.indexList=[]
            self.par=None
            #每个节点有一个唯一的整数值做为id,方便用graphviz绘图
            self.id = getGID()

    def isleaf(self):
        return False

    def isfull(self):
        return len(self.indexList)>=self.M-1

    def isempty(self):
        return len(self.indexList)<=(self.M+1)/2-1

    @property
    def M(self):
        return self.__M

#叶子节点       
class Bptree_Leaf(object):

    def __init__(self,L):
        if not isinstance(L,int):
            raise InitError('L must be int')
        else:
            self.__L=L
            self.valueList=[]
            self.bro=None
            self.par=None
            #每个节点有一个唯一的整数值做为id,方便用graphviz绘图
            self.id = getGID()

    def isleaf(self):
        return True

    def isfull(self):
        return len(self.valueList)>self.L

    def isempty(self):
        return len(self.valueList)<=(self.L+1)/2

    @property
    def L(self):
        return self.__L

#------------------ node 定义结束 ----------------------------------------

#B+树类        
class Bptree(object):
    def __init__(self,M,L):
        if L>M:
            raise InitError('L must be less or equal then M')
        else:
            self.__M=M
            self.__L=L
            self.__root=Bptree_Leaf(L)
            self.__leaf=self.__root

    @property
    def M(self):
        return self.__M

    @property
    def L(self):
        return self.__L

    #在树上查找
    def search(self,mi=None,ma=None):
        result=[]
        node=self.__root
        leaf=self.__leaf
        if mi is None and ma is None:
            raise ParaError('you need to setup searching range')
        elif mi is not None and ma is not None and mi>ma:
            raise ParaError('upper bound must be greater or equal than lower bound')
        def search_key(n,k):
            if n.isleaf():
                p=bisect_left(n.valueList,k)
                return (p,n)
            else:
                p=bisect_right(n.indexList,k)
                return search_key(n.childList[p],k)
        if mi is None:
            while True:
                for kv in leaf.valueList:
                    if kv<=ma:
                        result.append(kv)
                    else:
                        return result
                if leaf.bro==None:
                    return result
                else:
                    leaf=leaf.bro
        elif ma is None:
            index,leaf=search_key(node,mi)
            result.extend(leaf.valueList[index:])
            while True:
                if leaf.bro==None:
                    return result
                else:
                    leaf=leaf.bro
                    result.extend(leaf.valueList)
        else:
            if mi==ma:
                i,l=search_key(node,mi)
                try:
                    if l.valueList[i]==mi:
                        result.append(l.valueList[i])
                        return result
                    else:
                        return result
                except IndexError:
                    return result
            else:
                i1,l1=search_key(node,mi)
                i2,l2=search_key(node,ma)
                if l1 is l2:
                    if i1==i2:
                        return result
                    else:
                        result.extend(l.valueList[i1:i2])
                        return result
                else:
                    result.extend(l1.valueList[i1:])
                    l=l1
                    while True:
                        if l.bro==l2:
                            result.extend(l2.valueList[:i2+1])
                            return result
                        else:
                            result.extend(l.bro.valueList)
                            l=l.bro
    #遍历B+树的所有叶子节点
    def traversal(self):
        result=[]
        l=self.__leaf
        while True:
            result.extend(l.valueList)
            if l.bro==None:
                return result
            else:
                l=l.bro
    
    #显示B+树
    def show(self):

        def dotShow(tree):        
            q=deque()
            h=0
            q.append([self.__root,h])

            #生成childList对应的dot格式的文本串
            def childListDotStr(n):
                dotStr ='{'
                if n.childList==[]:
                    return '{}'
                else:
                    for i,k in enumerate(n.indexList):
                        dotStr +='<f%s>#%s|'%(n.childList[i].id,n.childList[i].id)
                    #childList比indexList多一个，要处理一下最右孩子指针               
                    dotStr +='<f%s>#%s}'%(n.childList[-1].id,n.childList[-1].id)
                return dotStr

            #生成childList对应的dot格式的文本串
            def childListEdgeStr(n):
                dotStr =''
                if n.childList==[]:
                    return ''
                else:
                    for i,k in enumerate(n.indexList):
                        dotStr +='node%s:f%s:s--node%s:e:n;\n'% (n.id,n.childList[i].id,n.childList[i].id)
                    #childList比indexList多一个，要处理一下最右孩子指针               
                    dotStr +='node%s:f%s:s--node%s:e:n;\n'% (n.id,n.childList[-1].id,n.childList[-1].id)
                return dotStr

            while True:
                try:
                    node,height=q.popleft()
                except IndexError:
                    return
                else:
                    if not node.isleaf(): #内部节点
                        #print node.indexList,'the height is',height
                        nodeText = str([k for k in node.indexList])
                        tree.dotStr += 'node%s [label = "{<e> #%s|%s| %s}" ];\n'% (node.id,node.id,nodeText,childListDotStr(node))
                        tree.dotStr += childListEdgeStr(node)
                        if height==h:
                            h+=1
                        q.extend([[n,h] for n in node.childList])
                    else: #叶节点
                        #print [v.key for v in node.valueList],'the leaf is,',height
                        nodeText = str([k.key for k in node.valueList])
                        tree.dotStr += 'node%s [label = "{<e> #%s|%s}" ];\n'% (node.id,node.id,nodeText)
        self.dotStr=''
        dotShow(self)
        print(self.svgStr())

    # 生成svg图形对应的文本串
    def svgStr(self):
        dotHeader ='''
        graph G
{       
        rankdir = TB;
        node [shape=record];
        '''
        dotStr = dotHeader + self.dotStr +'}'
        dotFile =open('BplusTree.dot','w')
        dotFile.write(dotStr)
        dotFile.close()
        #调用dot命令生成svg文件
        os.system('dot -Tsvg BplusTree.dot -o BplusTree.html')
        #取出svg图形文件的内容
        svgFile =open('BplusTree.html','r')
        svgStr = svgFile.read()
        svgFile.close()        
        return svgStr
    

    #插入操作
    def insert(self,key_value):
        
        def split_node(n1):            
            mid=self.M/2 #分裂点为度数的中点
            newnode=Bptree_InterNode(self.M)
            #新节点的数据是原节点的后半部分
            newnode.indexList=n1.indexList[mid:]
            newnode.childList=n1.childList[mid:]
            newnode.par=n1.par

            for c in newnode.childList:
                c.par=newnode

            if n1.par is None: #如果当前节点是根节点，则创建一个新的根节点
                newroot=Bptree_InterNode(self.M)
                println(' #%s 号内部节点分裂，键 %s 将复制（上升）到新的根节点 #%s 中'%(n1.id,n1.indexList[mid-1],newroot.id))
                newroot.indexList=[n1.indexList[mid-1]]
                newroot.childList=[n1,newnode]
                n1.par=newnode.par=newroot
                self.__root=newroot
            else: #如果当前节点不是根节点
                println(' #%s 号内部节点分裂，键 %s 将复制（上升）到父节点 #%s 中'%(n1.id,n1.indexList[mid-1],n1.par.id))
                i=n1.par.childList.index(n1)
                n1.par.indexList.insert(i,n1.indexList[mid-1])
                n1.par.childList.insert(i+1,newnode)
            n1.indexList=n1.indexList[:mid-1]
            n1.childList=n1.childList[:mid]

            return n1.par

        #叶子节点分裂
        def split_leaf(n2):
            mid=(self.L+1)/2 #分裂点为叶子节点度数+1的中点
            newleaf=Bptree_Leaf(self.L)
            newleaf.valueList=n2.valueList[mid:]
            if n2.par==None: #如果当前节点是既是叶子节点又是根节点，则创建一个新的内部节点
                newroot=Bptree_InterNode(self.M)
                println(' #%s 号叶子节点分裂，键 %s 将复制（上升）到新的根节点 #%s 中'%(n2.id,n2.valueList[mid].key,newroot.id))
                newroot.indexList=[n2.valueList[mid].key]
                newroot.childList=[n2,newleaf]
                n2.par=newleaf.par=newroot
                self.__root=newroot
            else:
                println(' #%s 号叶子节点分裂，键 %s 将复制（上升）到父节点 #%s 中'%(n2.id,n2.valueList[mid].key,n2.par.id))
                i=n2.par.childList.index(n2)
                n2.par.indexList.insert(i,n2.valueList[mid].key)
                n2.par.childList.insert(i+1,newleaf)
                newleaf.par=n2.par
            n2.valueList=n2.valueList[:mid]
            n2.bro=newleaf

        #插入节点
        def insert_node(n):
            println('对 #%s 号节点进行检查 '%(n.id))

            if not n.isleaf():
                println(' #%s 号节点是内部节点 '%(n.id))
                if n.isfull():
                    println(' #%s 号节点已满，分裂后再做插入操作 '%(n.id))
                    insert_node(split_node(n))
                else:                    
                    p=bisect_right(n.indexList,key_value)
                    #println(' 插入位置：%s '%p)
                    pp = 0 if p == 0  else p - 1
                    if p > 0:
                        println(' #%s 号节点未满，找到稍小于 %s 的键值 %s ,在 %s 的右孩子 #%s 号节点上执行插入操作'%(n.id,key_value.key,n.indexList[pp],n.indexList[pp],n.childList[p].id))
                    else:
                        println(' #%s 号节点未满，只能找到比 %s 稍大的键值 %s ,在 %s 的左孩子 #%s 号节点上执行插入操作'%(n.id,key_value.key,n.indexList[pp],n.indexList[pp],n.childList[p].id))
                    insert_node(n.childList[p])
            else:
                println(' #%s 号节点是叶子节点, 实际插入键值与卫星数据 '%(n.id))
                p=bisect_right(n.valueList,key_value)
                n.valueList.insert(p,key_value)
                if n.isfull():
                    println(' #%s 号叶子节点已满, 分裂该节点 '%(n.id))
                    split_leaf(n)
                else:
                    return

        insert_node(self.__root)    

def println(str):
    print('\n<br>')
    print(str)
    print('\n<br>')

#固定序列
def fixTestList():
    testlist=[]
    keyList =[10, 17, 9, 33, 33, 50, 36, 41, 31, 30, 13, 6, 37, 45, 20, 4, 35, 11, 2, 40]

    #通常情况下树高为3，下面这个序列会生成树高为4的B+树，可以分析一下为什么会这样？
    #keyList =[3, 33, 25, 30, 15, 27, 16, 35, 28, 39, 44, 2, 47, 45, 14, 42, 18, 3, 9, 18, 34, 19, 33, 46, 24, 45, 48, 20, 10, 8, 35, 3, 49, 48, 50, 9, 46, 1, 31, 6, 37, 34, 33, 37, 6, 48, 39, 24, 17] 
    for key in keyList:        
        value=choice('abcdefghijklmn')
        testlist.append(KeyValue(key,value))   
    println(str([k.key for k in testlist]))
    return testlist;

#随机序列，用50个数的随机序列生成B+树时，观察生成的图形，大部分时候树高都是3，但偶尔出现树高为4，是因为B+树在面对特定数据时树高会高一些吗？
def randTestList():
    testlist=[]
    for i in range(1,50):
        key=randint(1,50)
        value=choice('abcdefghijklmn')
        testlist.append(KeyValue(key,value))   
    println(str([k.key for k in testlist]))
    return testlist;

#测试插入操作                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
def testInsert():  

    M=4
    L=4
    #构造一个空的B+树
    mybptree=Bptree(M,L)

    println('B+树的插入过程, 内部%s阶，叶子%s阶 '%(M,L))

    println('插入序列')
    testlist = fixTestList()  
    #testlist = randTestList()    

    for kv in testlist:
        println('<br>------------ 准备插入 %s : -------------------------------'%kv.key)
        mybptree.insert(kv)
        println('插入 %s 后的B+树'%kv.key)
        mybptree.show()

    
if __name__=='__main__':
    testInsert()
