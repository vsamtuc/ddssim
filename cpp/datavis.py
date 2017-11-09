import sqlite3 as sql
from io import StringIO, TextIOWrapper
from subprocess import PIPE, Popen
from contextlib import closing
import csv



class Attribute(object):
    TypeMap = {
        int : 'INT',
        float : 'REAL',
        str: 'TEXT',
        None: ''
        }
    def __init__(self, name, atype=None):
        self.name = name
        if atype in self.TypeMap:
            self.type = self.TypeMap[atype]
        else:
            self.type = atype
    def sql_create(self):
        return ' '.join([self.name, self.type])


def sql_scalar(value):
    """
    Return a string mapping the python value to SQL representation.
    This function will currently only accept str, int and float.
    """
    
    assert isinstance(value, (str,int,float))
    # for now, this is naive 
    if isinstance(value, str):
        return "'"+value+"'"
    else:            
        return repr(value)


class Selector(object):
    """
    Selector is a utility class implementing SQL predicate generators.
    
    Usage: 
    sel = Selector(" character_length(%(attribute)s) > 4 ")
    sel('a.name') ->  'character_length(a.name) > 4'
    sel('a.name') & sel('b.name')  -> 'character_length(a.name) > 4 AND character_length(a.name) > 4'
    """
    
    def __init__(self, pat):
        """pat is a string. I
        """
        self.pat = pat
        
    def __call__(self,attr):
        return self.pat % {'attribute' : attr}
    def __and__(self,other):
        if not isinstance(other,Selector): return NotImplemented
        return AND(self,other)
    def __or__(self,other):
        if not isinstance(other,Selector): return NotImplemented
        return OR(self,other)


def approx(x, epsilon=1E-4):
    """Return a Selector for abs(%attr - x)<=epsilon*x""" 
    if x==0:
        return Selector("abs(%%(attribute)s)<=%s" % sql_scalar(epsilon))
    else:
        return Selector("abs(%%(attribute)s/%s - 1)<=%s" % (sql_scalar(x),sql_scalar(epsilon)))

def sql_binary_operator(x, op):
    """Return a Selector for '%attr op x'""" 
    return Selector("%%(attribute)s %s %s" % (op,sql_scalar(x)))

def sql_boolean_binary_operator(op,*terms):
    """
    terms must be a non-empty list of Selectors, s1,...,sk.
    Returns a new Selector  of the form
    '(s1.pat op s2.pat ... op sk.pat)'
    This selector  can be used with op in ('AND', 'OR')
    """
    assert terms and all(isinstance(t,Selector) for t in terms)
    return Selector("(" + (" "+op+" ").join(t.pat for t in terms) + ")")

def less_than(x): return sql_binary_operator(x,"<")
def less_equal(x): return sql_binary_operator(x,"<=")
def greater_than(x): return sql_binary_operator(x,">")
def greater_equal(x): return sql_binary_operator(x,">=")
def not_equal(x): return sql_binary_operator(x,"<>")
def like(x): return sql_binary_operator(x,"LIKE")
def not_like(x): return sql_binary_operator(x,"NOT LIKE")
def between(a,b): 
    return Selector("%%(attribute)s BETWEEN %s AND %s" % (sql_scalar(a),sql_scalar(b)))

def AND(*terms):
    """
    terms: a list of Selector objects
    
    Return a Selector for the conjunction of terms.
    Usage:
    
    AND(greater_than('A'), less_than('E'))  
    """
    return sql_boolean_binary_operator("AND",*terms)

def OR(*terms):
    """
    terms: a list of Selector objects
    
    Return a Selector for the disjunction of terms.
    Usage:
    
    OR(greater_than('E'), less_than('C'))  
    """
    return sql_boolean_binary_operator("OR",*terms)


#
# Database schema objects
#

class Relation(object):
    """
    A base class for Table and View.
    
    Relation has a name and a set of attributes. It is used to compose
    SQL SELECT queries over db tables and views. 
    """
    def __init__(self, name, alist):
        self.name = name
        self.set_attributes(alist)

    def set_attributes(self, alist):
        assert all(isinstance(a, Attribute) for a in alist)
        self.attributes = alist
        
    def sql_fetch_all(self):
        return self.sql_select(['*'])        

    def __map_value(self, attr, value):
        if isinstance(value, (list, tuple)):
            return ("%s IN (" % attr) + ",".join(sql_scalar(v) for v in value) + ")"
        elif isinstance(value, Selector):
            return value(attr)
        else:
            return "%s=%s" % (attr, sql_scalar(value))

    def sql_select_clause(self, alist, distinct):
        return "SELECT " + ("DISTINCT " if distinct else "") + ','.join(alist)
    def sql_from_clause(self):
        return "FROM "+self.name
    def sql_where_clause(self, where):
        if where:
            if isinstance(where, dict):
                # make it into a list
                where = [(a,where[a]) for a in where]
            if isinstance(where, list):
                return " WHERE " + ' AND '.join([self.__map_value(attr,value) for attr,value in where])
        return ""
    def sql_order_by_clause(self,order):
        if order:
            return " ORDER BY "+','.join(order)
        return ""

    def sql_select(self, alist, where=None, order=[], distinct=False):
        """
        alist: a list of names or attributes
        where: a list of (attr, value) or a dict { attr: value }. Value can be an int,double,str,unicode, or a list of these
        order: a sublist of alist
        """

        sql = ' '.join([self.sql_select_clause(alist, distinct),
                        self.sql_from_clause(),
                        self.sql_where_clause(where),
                        self.sql_order_by_clause(order)
                        ])
        print(sql)
        return sql

    def scheme(self):
        return ''.join([self.name,'(',','.join("%s %s" % (a.name, a.type) for a in self.attributes),')'])

    def axis_values(self, attr):
        return self.dataset.axis_values(self, axes)


class Table(Relation):
    """
    Table objects map SQL tables to python. They inherit from relations.
    Table objects can be used to create and load SQL tables.
    """
    def __init__(self, name, alist):
        Relation.__init__(self, name, alist)
    def sql_create(self):
        """Return a CREATE TABLE query for this table."""
        return ' '.join(['CREATE TABLE',self.name, '(',
                         ', '.join(a.sql_create() for a in self.attributes),
                         ')'])
    

    
    def sql_insertmany(self):
        """Return an INSERT ... VALUES query for this table."""
        return "INSERT INTO "+self.name+" VALUES(" + ','.join('?'*len(self.attributes))+")"

class View(Relation):
    """
    View object map SQL views to python. They inherit from Relation.
    They can be used to create SQL views.
    """
    def __init__(self, name, defquery):
        """
        name is the name of the view.
        defquery is an SQL query defining the view.
        Example:
        View('foo','SELECT A, B from BAR ORDER BY a)
        """
        Relation.__init__(self, name, [])
        self.defquery = defquery
    def sql_create(self):
        """Return a CREATE VIEW query for this view."""
        return ' '.join(['CREATE VIEW',self.name,'AS',self.defquery])
        

class Dataset(object):
    """
    A Dataset encapsulates a new in-memory database.
    
    Dataset objects can be used to add Table and View objects to the
    database, load data into tables and access tables and views by name.
    """
    def __init__(self):
        self.conn = sql.connect(':memory:')
        self.relations = {}

    def add(self, relation):
        """
        Add relation to the dataset.
        """
        assert isinstance(relation, Relation)
        self.conn.execute(relation.sql_create())
        self.relations[relation.name] = relation
        relation.dataset = self
        setattr(self, relation.name, relation)
        return relation
                   
    def create_table(self, name, alist):
        """
        Create and add a Table, given name and attribute list.
        """
        return self.add(Table(name, alist))

    def __get_attributes_of_relation(self, name):
        c = self.conn.cursor()
        try:
            c.execute("SELECT * FROM "+name)
            alist = [Attribute(a[0],a[1]) for a in c.description]
            return alist
        finally:
            c.close()            

    def create_view(self, name, qry):
        """
        Create view and add it to the query,
        """
        view= self.add(View(name, qry))
        view.set_attributes(self.__get_attributes_of_relation(name))
        return view
        

    def load_csv(self, filename, dialect='excel', **fmtargs):
        """
        Load data from a CSV file.
        
        Arguments dialect and fmtargs are passed to tbe standard csv.reader
        """
        tabdata = {}
        with open(filename,"r") as fin:
            reader = csv.reader(fin, dialect, **fmtargs)
            for row in reader:
                if row[0] not in tabdata:
                    tabdata[row[0]]=[]
                tabdata[row[0]].append(row[1:])
                
        for tabname in tabdata:
            table = self.relations[tabname]
            assert isinstance(table,Table)
            self.conn.executemany(table.sql_insertmany(), tabdata[tabname])

    def load_csv_in_table(self, rel, filename, dialect='excel', **fmtargs):
        data=[]
        with open(filename,"r") as fin:
            reader = csv.reader(fin, dialect, **fmtargs)
            data = [row for row in reader]

        table = self.relations[rel]
        self.conn.executemany(table.sql_insertmany(), data)
    	

    def print_relation(self, name):
        rel = self.relations[name]
        print(rel.scheme())
        for row in self.conn.execute(rel.sql_fetch_all()):
            print(row)

    def axis_values(self, rel, attr):
        """
        Return the values of the given attribute that appear in the given relation.
        
        rel is either the name of a relation or a Relation object.
        attr is either the name of an attribute or an Attribute object, or a list thereof.
        """
        if not isinstance(rel, Relation):
            rel = self.relations[rel]
        if not isinstance(attr, (list,tuple)):
            attr = [attr]
        sql = rel.sql_select(attr, order=attr, distinct=True)
        return [val for val in self.conn.execute(sql)]


class Plot(object):
    def __init__(self, title=None, xlabel=None, 
                 ylabel=None, x_range=None, y_range=None, terminal=None, 
                 logscale=None, grid=" ", key=None, output=None):
        self.graphs=[]
        self.title=title
        self.xlabel=xlabel
        self.ylabel=ylabel
        self.x_range = x_range
        self.y_range = y_range
        self.terminal=terminal
        self.logscale=logscale
        self.grid=grid
        self.key=key
        self.output=output
        
    def make(self, gnuplot="gnuplot"):
        p = Popen(gnuplot, stdin=PIPE)
        try:
            wrapper = TextIOWrapper(p.stdin)
            self.make_script(wrapper)
        finally:
            wrapper.close()
            p.stdin.close()

    def make_script(self, f):
        if self.title:      print('set title', '"'+self.title+'"', file=f)
        if self.xlabel:     print('set xlabel "',self.xlabel,'"', file=f)
        if self.ylabel:     print('set ylabel "',self.ylabel,'"', file=f)
        if self.x_range:    print('set xrange ',self.x_range, file=f)
        if self.y_range:    print('set yrange ',self.y_range, file=f)
        if self.logscale:   print('set logscale %s' % self.logscale, file=f)
        if self.grid:       print('set grid %s' % self.grid, file=f)
        if self.key:        print("set key %s" % self.key, file=f)
        if self.terminal:   print(self.terminal.out(self.output), file=f)
        
        print("plot", ','.join([g.output_plot() for g in self.graphs]), file=f)
        for g in self.graphs:
            g.output_data(f)
            
        return f

    def add_graph(self, rel, x, y, select={}, title=None, style='linespoints'):
        g  = Graph(self, rel, x, y, select=select, title=title, style=style)
        return self


class Graph(object):
    def __init__(self, plot, rel, x, y, select=[], title=None, style='linespoints'):
        self.plot = plot
        plot.graphs.append(self)

        self.relation = rel
        assert isinstance(rel, Relation) and rel.dataset is not None
        
        self.x = x
        self.y = y
        
        self.select = select
        self.title = title
        self.style = style

    def __default_title(self):
        return str(self.select)

    def __output_style(self):
        if callable(self.style):
            return self.style(self.select)
        else:
            return str(self.style)
            
    def output_plot(self):        
        title = self.__default_title() if self.title is None else self.title
        style = "" if self.style is None else "with "+self.__output_style()
        return  """ '-' title "%s" %s""" % (title,style)

    def output_data(self, f):
        sql = self.relation.sql_select([self.x,self.y], where=self.select, order=[self.x])
        conn = self.relation.dataset.conn
        for row in conn.execute(sql):
            print(" ".join(str(x) for x in row),file=f)
        print("end", file=f)


        
#
# Gnuplot terminals
#

class PNG(object):
    def __init__(self,filename=None,size=(1024,768)):
        self.filename = filename
        self.size = size
    def out(self, filebase=None):
        fname = self.filename if self.filename else filebase+'.png' if filebase else None
        
        ret = "set terminal png size %d,%d\n" % self.size
        if fname: 
            ret += "set output \""+fname+"\"\n"
        return ret

class Tikz(object):
    def __init__(self, filename=None, size=None):
        self.filename = filename
        self.size = size
    def out(self,filebase=None):
        fname = self.filename if self.filename else filebase+'.tex' if filebase else None
        
        ret = "set terminal tikz"
        if self.size:
            ret += " size %s" % self.size
        ret += "\n"
        if fname: 
            ret += "set output \""+fname+"\"\n"
        return ret        

#
# Plotting utilities
#

DEFAULT='DEFAULT'


def __default_title(x, y, axes, axisvalues):
    title = "   "+x+" over "+y+"   "
    sva = []
    for a in axes:
        if len(axisvalues[a])==1:
            sva.append((a, list(axisvalues[a])[0]))
    if sva:
        title += "("+ ','.join("%s=%s" % (a,str(v)) for a,v in sva) + ")"
    return title


def make_plot(rel, x, y, axes, select={}, title=DEFAULT, style='linespoints',  
              legend=DEFAULT, 
              xlabel=None, ylabel=None, x_range=None, y_range=None, 
              terminal=None, logscale=None, grid=" ", key=None,
              output=DEFAULT):
    """
    Return a Plot object, containing a number of Graph objects.
    
    The graphs contained in the plot are produced as follows:
    First, the following query (in pseudo-SQL) is executed
    SELECT DISTINCT axes... FROM rel WHERE select... ORDER BY axes...
    
    For each result row R of this query, a new graph is created. The
    data used for the graph is retrieved as follows
    SELECT x, y FROM rel WHERE axes...=R... ORDER BY x     
    
    rel: the Relation from which data will be drawn  
    x,y: the plotted attribute names
    axes: a list of attribute names which parameterize each Graph 
    select: a dict of (a, v) where a is an axis name and v is either a value,
         a list of values or a Selector.
    title: a string to be used as Plot title. If not given, a title is created automatically.
    style: this is passed to the Graph objects
    legend: a string, used to produce the legend for each Graph.
    xlabel, x_range, ylabel, y_range: strings passed to gnuplot
    logscale: a string passed to gnuplot
    grid: a string passed to gnuplot
    terminal: an object used to select and configure a gnuplot terminal
    output: a string used to create output file of gnuplot    
    """

    # set of per-attribute values
    axisvalues = { axis:set([]) for axis in axes }
        
    # compute set of all axis values
    axisdata = rel.dataset.conn.execute(rel.sql_select(axes, where=select, order=axes,distinct=True)).fetchall()

    # fill axis values
    for row in axisdata:
        for axis,value in zip(axes,row):
            axisvalues[axis].add(value)
        
    # set the plot title
    if title is DEFAULT:
        title = __default_title(x,y,axes,axisvalues)

    if output is DEFAULT:
        wc = rel.sql_where_clause(select)
        output = rel.name+':'+y+'('+x+')'+wc
        output = output.replace(' ','.')

    if xlabel is None: xlabel = x
    if ylabel is None: ylabel = y

    # prepare the legend titles
    if legend is DEFAULT:
        legend = ','.join("%s=%%(%s)s" % (axis, axis) for axis in axes if len(axisvalues[axis])>1)

    # create the plot
    plot = Plot(title=title, 
                xlabel=xlabel, ylabel=ylabel, 
                x_range=x_range, y_range=y_range, 
                terminal=terminal, logscale=logscale, grid=grid, key=key,
                output=output)
        
    # make the graphs
    for row in axisdata:
        sel = dict(zip(axes,row))
        plot.add_graph(rel, x, y, select=sel, title=(legend%sel), style=style)

    return plot

    
    

if __name__=='__main__':
    print("""Example usage:
    alist = [
        ('dset', str),
        ('theta', float),
        ('epsilon', float),
        ('servers',int),
        ('buckets',int),
        ('depth',int),
        ('bytes_sent',float),
        ('bytes_total',float),
        ('msg_sent',float),
        ('msg_total',float)]

    attlist = [Attribute(name, atype) for name,atype in alist]
    ds = Dataset()

    ds.create_table('TABLE1', attlist)
    ds.create_table('TABLE2',attlist)
    ds.load_csv('mydata.dat')
    ds.create_view('VIEW1', 
               "SELECT *, epsilon/theta as eratio FROM TODS")

    for rel in ds.relations:
        ds.print_relation(rel)
        
    with open('test.gp','w') as f:
        plot = make_plot(ds.VIEW1,'eratio','bytes_sent',
                         axes=['theta','dset','servers'],
                         select={'theta':less_than(0.125)&greater_than(0.115), 'servers':between(3,6)},
                         terminal=PNG())
        plot.make_script(f)
""")
