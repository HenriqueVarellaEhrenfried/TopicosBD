require 'fileutils'

numVezes = ARGV[0]

directory_name = "Resultados"
Dir.mkdir(directory_name) unless File.exists?(directory_name)
files = [
            "Obras_por_estágio.svg",
            "Obras_por_unidades_da_federação.svg",
            "Obras_por_unidades_da_federação_e_estágio.svg",
            "Porcentagem_de_erro_da_predição.svg",
            "results.txt"
        ]

(0..numVezes.to_i).to_a.each do |run|
    directory_name = "Rodada #{run}"
    Dir.mkdir(directory_name) unless File.exists?(directory_name)
    system("python bd-tp1.py")
    files.each do |f|
        FileUtils.mv(f, directory_name)
    end
   FileUtils.mv(directory_name, "Resultados")
end