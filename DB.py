import csv
import sys
import re

def Get_Metadata(file_name):
    f=open(file_name,'r');
    regex='<begin_table>\r\s((.+?\r\s)+?)<end_table>'
    Metadata={};
    matches = re.finditer(regex, f.read())
    for match_number,match in enumerate(matches):
        Data=match.group(1).strip('\r\n').split('\r\n')
        Metadata[Data[0]]=Data[1:]
    return Metadata

Metadata=Get_Metadata('metadata.txt')

def findColTable(col,Tables):
    unknown_table=[];
    for table in Tables:
        # print col,Metadata[table][0]
        if col in Metadata[table]:
            unknown_table.append(table);
    if(len(unknown_table)==1):
        return str(unknown_table[0])+'.'+col
    else:
        if(len(unknown_table)==0):
            print "There is no column name "+col;
        else:
            print "column name "+col+' is in multiple tables';
        sys.exit();

def AddTableNametoCol(col,Tables):
    column=re.match('^(\w+)\.(\w+)$',col);
    if(column==None):
        column=re.match('^\w+$',col);
        if column==None:
            print "There is no column name "+col;
            sys.exit()
        return findColTable(col,Tables);
    else:
        if column.group(1) in Metadata.keys():
            if column.group(2) in Metadata[column.group(1)]:
                return col;
            else:
                print "There is no column name "+col;
        else:
            print "There is no column name "+col;


def get_table_data(table_name):
    f=open(str(table_name)+'.csv','r');
    reader=csv.reader(f);
    TableData=[];
    for row in reader:
        TableData.append(row)
    f.close();
    return TableData;

def Cross_Join(T):
    if(len(T)==1):
        return get_table_data(T[0]);
    else:
        Data=[];
        Table_Data=get_table_data(T[0]);
        for row in Table_Data:
            Merge_Table=Cross_Join(T[1:]);
            for row1 in Merge_Table:
                Data.append(row+row1);
        return Data;

def ExtractSingleCondition(Conditions,Tables):
    parse=re.match('(.*)(<=|>=|!=)(.*)',Conditions);
    if parse==None:
        parse=re.match('(.*)([<=>])(.*)',Conditions);
        if parse==None:
            print "Wrong Conditions"
            sys.exit();
    condition=parse.group(2);
    column1=AddTableNametoCol(re.sub('\s+','',parse.group(1)),Tables);
    column2=re.sub('\s+','',parse.group(3));
    if not column2.isdigit():
        column2=AddTableNametoCol(column2,Tables);

    return (column1,column2,condition)
    # return Conditions

def Extract_Conditions(Conditions,Tables):
    ParsedConditions={};
    parse=re.match('(.*)\s+and\s+(.*)',Conditions);
    if parse==None:
        parse=re.match('(.*)\s+or\s+(.*)',Conditions);
        if(parse==None):
            ParsedConditions=['normal',ExtractSingleCondition(Conditions,Tables)];
        else:
            ParsedConditions=['or',ExtractSingleCondition(parse.group(1),Tables),ExtractSingleCondition(parse.group(2),Tables)]
    else:
        ParsedConditions=['and',ExtractSingleCondition(parse.group(1),Tables),ExtractSingleCondition(parse.group(2),Tables)]

    return ParsedConditions

def Evaluate(Value1,Value2,condition):
    if(condition=='='):
        return Value1==Value2;
    if(condition=='>='):
        return Value1>=Value2;
    if(condition=='<='):
        return Value1<=Value2;
    if(condition=='!='):
        return Value1!=Value2;
    if(condition=='>'):
        return Value1>Value2;
    if(condition=='<'):
        return Value1<Value2;

def Temp(Mdata,Conditions):
    index1=Mdata.index(Conditions[0])
    isIndex2Value=False;
    if(Conditions[1].isdigit()):
        isIndex2Value=True;
        Value2=int(Conditions[1]);
        return(index1,isIndex2Value,Value2)
    else:
        index2=Mdata.index(Conditions[1]);
        return (index1,isIndex2Value,index2)

def Check_Conditions(Mdata,Data,Conditions):
    # print Conditions[0]
    index1,isIndex2Value,temp2=Temp(Mdata,Conditions[1]);
    if(isIndex2Value):
        Value2=temp2;
    else:
        index2=temp2;

    if(Conditions[0]=='and' or Conditions[0]=='or'):
        index3,isIndex4Value,temp2=Temp(Mdata,Conditions[2]);
        if(isIndex4Value):
            Value4=temp2;
        else:
            index4=temp2;

    RequiredData=[];
    for row in Data:
        Value1=row[index1];
        if not isIndex2Value:
            Value2=row[index2];
        if(Conditions[0]=='and' or Conditions[0]=='or'):
            Value3=row[index1];
            if not isIndex4Value:
                Value4=row[index4];
        # print Value1,Value2
        if(Conditions[0]=='normal'):
            print Conditions[1][2]
            if (Evaluate(int(Value1),int(Value2),Conditions[1][2])):
                RequiredData.append(row);
        if(Conditions[0]=='and'):
            if (Evaluate(int(Value1),int(Value2),Conditions[1][2]) and Evaluate(int(Value3),int(Value4),Conditions[2][2])):
                RequiredData.append(row);
        if(Conditions[0]=='or'):
            if (Evaluate(int(Value1),int(Value2),Conditions[1][2]) or Evaluate(int(Value3),int(Value4),Conditions[2][2])):
                RequiredData.append(row);

    return RequiredData;

def ExecuteSum(Mdata,FinalData,ColumnsToSelect):
    index=Mdata.index(ColumnsToSelect[0]);
    Sum=0;
    for row in FinalData:
        Sum=Sum+int(row[index]);
    print 'sum('+ColumnsToSelect[0]+')'
    print Sum;

def ExecuteMax(Mdata,FinalData,ColumnsToSelect):
    index=Mdata.index(ColumnsToSelect[0]);
    Max=int(FinalData[0][index]);
    for row in FinalData:
        Max=max(Max,int(row[index]));
    print 'MAX('+ColumnsToSelect[0]+')'
    print Max;

def ExecuteMin(Mdata,FinalData,ColumnsToSelect):
    index=Mdata.index(ColumnsToSelect[0]);
    Min=int(FinalData[0][index]);
    for row in FinalData:
        Min=min(Min,int(row[index]));
    print 'MIN('+ColumnsToSelect[0]+')'
    print Min;


def ExecuteAvg(Mdata,FinalData,ColumnsToSelect):
    index=Mdata.index(ColumnsToSelect[0]);
    Sum=0;
    for row in FinalData:
        Sum=Sum+int(row[index]);
    print 'sum('+ColumnsToSelect[0]+')'
    print Sum/len(FinalData);

def Print_Data(Mdata,Data,ColumnsToSelect,isSelectDistinctcommand):
    indices=[];
    for column in ColumnsToSelect:
        indices.append(Mdata.index(column));
    print(','.join([Mdata[i] for i in indices]));
    FinalData=[]
    for row in Data:
        FinalData.append([row[i] for i in indices]);

    if(isSelectDistinctcommand):
        FinalData=[list(x) for x in set(tuple(x) for x in FinalData)]
    for row in FinalData:
        print (','.join(data for data in row));


def Select_Command(Command):
    isSelectcommand=(re.match('^select[ \t]+',Command)!=None)
    isSelectDistinctcommand=(re.match('^select[ \t]+distinct[ \t]+',Command)!=None)
    # print isSelectcommand
    # if the command is not select then Exit;
    if(not isSelectcommand):
        print "Not a Select_Command"
        sys.exit();
    if(isSelectDistinctcommand):
        Command=re.sub('^select[ \t]+distinct[ \t]+','',Command);
    else:
        Command=re.sub('^select[ \t]+','',Command);
    parse=re.match('(.*)[ \t]+from[ \t]+(.*)',Command);

    # if the command have no from then Exit;
    if(parse==None):
        print "there is no from in the select command"
        sys.exit();

    # if the command have no columns to select then Exit;
    if(parse.group(1)==''):
        print "command have no columns to select"
        sys.exit();

    # if the command have no tables to select then Exit;
    if(parse.group(2)==''):
        print "command have no tables to select"
        sys.exit();

    SelectEverything=False;FindMax=False;
    FindMin=False;FindSum=False;FindAvg=False;
    Columns=re.sub('[ \t]+','', parse.group(1)).split(',')
    ColumnsToSelect=[];
    if len(Columns)==1:
        if Columns[0]=='*':
            SelectEverything=True;
        elif re.match('max\((.*)\)',Columns[0]):
            FindMax=True;
            Columns[0]=re.match('max\((.*)\)',Columns[0]).group(1);
        elif re.match('min\((.*)\)',Columns[0]):
            FindMin=True;
            Columns[0]=re.match('min\((.*)\)',Columns[0]).group(1);
        elif re.match('avg\((.*)\)',Columns[0]):
            FindAvg=True;
            Columns[0]=re.match('avg\((.*)\)',Columns[0]).group(1);
        elif re.match('sum\((.*)\)',Columns[0]):
            FindSum=True;
            Columns[0]=re.match('sum\((.*)\)',Columns[0]).group(1);

    # print Columns
    Tables_with_Conditions=parse.group(2);

    parse=re.match('(.*)[ \t]+where[ \t]+(.*)',Tables_with_Conditions);
    Conditions=[];Tables=[];

    if(parse==None):
        Tables=re.sub('[ \t]+','', Tables_with_Conditions).split(',')
    else:
        Tables=re.sub('[ \t]+','', parse.group(1)).split(',')
        Conditions=Extract_Conditions(parse.group(2),Tables);

    # print Conditions
    for table in Tables:
        if table not in Metadata.keys():
            print "No such Table Exists"
            sys.exit()
        else:
            if(SelectEverything):
                ColumnsToSelect.extend([str(table)+'.'+s for s in Metadata[table]])

    if(not SelectEverything):
        for col in Columns:
            ColumnsToSelect.append(AddTableNametoCol(col,Tables));

    # print ColumnsToSelect
    Mdata=[];
    for table in Tables:
        Mdata=Mdata+[str(table)+'.'+s for s in Metadata[table]];

    Data= Cross_Join(Tables);
    if (Conditions):
        FinalData=Check_Conditions(Mdata,Data,Conditions)
    else:
        FinalData=Data;
    if(FindSum):
        ExecuteSum(Mdata,FinalData,ColumnsToSelect)
    elif(FindAvg):
        ExecuteAvg(Mdata,FinalData,ColumnsToSelect)
    elif(FindMax):
        ExecuteMax(Mdata,FinalData,ColumnsToSelect)
    elif(FindMin):
        ExecuteMin(Mdata,FinalData,ColumnsToSelect)
    else:
        Print_Data(Mdata,FinalData,ColumnsToSelect,isSelectDistinctcommand);

# Command=raw_input('Enter SQL Command:')
Command=sys.argv[1];
Select_Command(Command)
