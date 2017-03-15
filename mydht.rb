# Software developed by Henrique Varella Ehrenfried
# for the course of Database Topics (CI 809) in the 
# Federal University of Parana (UFPR) under requests
# of Professor Doctor Eduardo Cunha de Almeida
#------------------------------------------------------
# Constants
NUMBER_NODE_MAX = 10
TABLE_FIRST_NODE = 0

# Global Variables
$node_id_counter = 0

class Node
    def initialize(id = 0, next_node = nil, previous_node = nil )
        @id = id
        @next_node = next_node
        @previous_node = previous_node
        @keys = []
        @number_of_keys = 0
    end
    def updateNode(next_node = nil, previous_node = nil)
        @next_node = next_node
        @previous_node = previous_node 
    end
    def canStoreKey?(k)
        return ((((@id - 1) < k)) and (k <= @id)) ? true : false
    end
    def isFirstNode?
        return (@previous_node > @id) ? true : false
    end
    attr_reader :id
    attr_reader :next_node
    attr_reader :previous_node
    attr_reader :keys
    attr_reader :number_of_keys
end
class Table
    def initialize
        @nodes = []
        @firstNode = 0
        for i in @firstNode..NUMBER_NODE_MAX-1 do
            @nodes << Node.new(i,i+1,i-1)
        end
        @nodes.last.updateNode(@nodes.first.id, @nodes[-2].id)
    end   
    def nodeEnter

    end
    def nodeLeave
        
    end
    # i start at 0; m is the number of bits to store the biggest key
    def calculateTableEntry(id, i, m)
        return((id + (2 ** i)) %  (2 ** m))
    end
    
end
class Utility
    def initialize
        @table = Table.new
    end   
    def main
        # Initialize 
        input = []
        STDIN.each_line do |line|
            input = line.split(" ")
            p input
        end
    end
end

main = Utility.new 
main.main


#-------------------
# -----Notes--------
# attr_reader => Define getter
# attr_accessor => Define getter and setter
#-------------------