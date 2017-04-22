require 'csv'

aeroporto = CSV.read("../data_pac/aeroporto.csv")
infraturistica = CSV.read("../data_pac/infraturistica.csv")
cidadeshistoricas = CSV.read("../data_pac/cidadeshistoricas.csv")
cidadeshistoricas.shift
infraturistica.shift

csvFinal = aeroporto + infraturistica + cidadeshistoricas 

csvFinal.first << "type" # 4
csvFinal.first << "responsable" # 5
csvFinal.first << "uf_code" # 7
csvFinal.first << "stages" # 11

# ["FID", "codigo", "empreendimento", "subeixo", "tipo", "orgao_responsavel", "executor", "unidade_federativa", "municipio", "investimento_previsto", "obser
# vacao", "estagio", "data_de_referencia", "geometria", "count", "uf_code", "type", "responsable", "stages"]
dicionarioType = {}
dicionarioResponsable = {}
dicionarioUF = {}
dicionarioStage = {}

typeID = 0
responsableID = 0
ufID = 0
stagesID = 0

csvFinal.each do |c|
    unless dicionarioType.has_key? c[4]
        dicionarioType[c[4]] = typeID
        typeID += 1
    end
    unless dicionarioResponsable.has_key? c[5]
        dicionarioResponsable[c[5]] = responsableID
        responsableID += 1
    end
    unless dicionarioUF.has_key? c[7]
        dicionarioUF[c[7]] = ufID
        ufID += 1
    end
    unless dicionarioStage.has_key? c[11]
        dicionarioStage[c[11]] = stagesID
        stagesID += 1
    end
end

csvFinal[0].each_with_index {|a,i| puts ("#{a} => #{i}") }

for x in (1..csvFinal.length-1) do
    csvFinal[x] << dicionarioType[csvFinal[x][4]]
    csvFinal[x] << dicionarioResponsable[csvFinal[x][5]]
    csvFinal[x] << dicionarioUF[csvFinal[x][7]]
    csvFinal[x] << dicionarioStage[csvFinal[x][11]]
end
File.open("dadosTurísticos.csv", "w") {|f| f.write(csvFinal.inject([]) { |csv, row|  csv << CSV.generate_line(row) }.join(""))}
