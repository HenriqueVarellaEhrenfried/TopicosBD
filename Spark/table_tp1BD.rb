require 'csv'
data = [[2, 'AC - Em obras'], [2, 'AL - Em execução'], [3, 'AL - Concluído'], [3, 'AL - Em licitação de obra'], [6, 'AL - Em obras'], [10, 'AL - Ação preparatória'], [3, 'AM - Em execução'], [4, 'AM - Ação preparatória'], [5, 'AM - Em obras'], [5, 'AM - Concluído'], [1, 'AP - Em obras'], [1, 'BA - Concluído'], [7, 'BA - Ação preparatória'], [12, 'BA - Em obras'], [29, 'BA - Em execução'], [4, 'CE - Ação preparatória'], [5, 'CE - Em obras'], [16, 'CE - Em execução'], [1, 'ES - Em obras'], [3, 'ES - Ação preparatória'], [3, 'GO - Em execução'], [4, 'GO - Concluído'], [5, 'GO - Em obras'], [1, 'MA - Ação preparatória'], [4, 'MA - Concluído'], [8, 'MA - Em obras'], [34, 'MA - Em execução'], [1, 'MG - Concluído'], [2, 'MG - Em licitação de projeto'], [12, 'MG - Ação preparatória'], [18, 'MG - Em obras'], [69, 'MG - Em execução'], [1, 'MS - Ação preparatória'], [2, 'MS - Concluído'], [2, 'MS - Em obras'], [6, 'MS - Em execução'], [1, 'MT - Em licitação de projeto'], [1, 'MT - Em execução'], [4, 'MT - Ação preparatória'], [11, 'MT - Em obras'], [3, 'PA - Concluído'], [3, 'PA - Em obras'], [4, 'PA - Em execução'], [4, 'PA - Ação preparatória'], [6, 'PA - Em licitação de projeto'], [1, 'PB - Concluído'], [2, 'PB - Em obras'], [2, 'PB - Em licitação de obra'], [4, 'PB - Ação preparatória'], [4, 'PB - Em execução'], [1, 'PE - Concluído'], [4, 'PE - Em licitação de projeto'], [7, 'PE - Em obras'], [9, 'PE - Em execução'], [10, 'PE - Ação preparatória'], [2, 'PI - Em execução'], [6, 'PI - Ação preparatória'], [1, 'PR - Ação preparatória'], [6, 'PR - Em obras'], [9, 'PR - Em execução'], [1, 'RJ - Concluído'], [1, 'RJ - Em licitação de projeto'], [5, 'RJ - Ação preparatória'], [8, 'RJ - Em obras'], [13, 'RJ - Em execução'], [1, 'RN - Em obras'], [4, 'RN - Ação preparatória'], [6, 'RN - Em execução'], [3, 'RS - Em licitação de projeto'], [4, 'RS - Concluído'], [7, 'RS - Em obras'], [8, 'RS - Ação preparatória'], [18, 'RS - Em execução'], [4, 'SC - Ação preparatória'], [7, 'SC - Em obras'], [9, 'SC - Em execução'], [2, 'SE - Em licitação de obra'], [2, 'SE - Ação preparatória'], [4, 'SE - Em obras'], [6, 'SE - Em execução'], [1, 'SP - Em licitação de obra'], [3, 'SP - Concluído'], [4, 'SP - Ação preparatória'], [7, 'SP - Em execução'], [8, 'SP - Em obras']]

uf =[['AP', 1], ['AC', 2], ['ES', 4], ['PI', 8], ['MS', 11], ['RN', 11], ['GO', 12], ['PB', 13], ['SE', 14], ['PR', 16], ['AM', 17], ['MT', 17], ['SC', 20], ['PA', 20], ['SP', 23], ['AL', 24], ['CE', 25], ['RJ', 28], ['PE', 31], ['RS', 40], ['MA', 47], ['BA', 49], ['MG', 102]]

estagio = [['Em licitação de obra', 8], ['Em licitação de projeto', 17], ['Concluído', 33], ['Ação preparatória', 98], ['Em obras', 129], ['Em execução', 250]]

CSV.open("my_table.csv", "wb") do |csv|
    csv << ["Quantidade", "UF/Estágio"]
    data.each do |d|
        csv << d
    end
end

CSV.open("my_table2.csv", "wb") do |csv|
    csv << ["UF", "Quantidade"]
    uf.each do |u|
        csv << u
    end
end

CSV.open("my_table3.csv", "wb") do |csv|
    csv << ["Estágio", "Quantidade"]
    estagio.each do |e|
        csv << e
    end
end

CSV.open("my_table.csv", "wb") do |csv|
    csv << ["Quantidade", "UF/Estágio"]
    data.each do |d|
        csv << d
    end
end