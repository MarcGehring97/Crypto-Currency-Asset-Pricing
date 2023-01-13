data_path = "/Users/Marc/Desktop/Past Affairs/Past Universities/SSE Courses/Master Thesis/Data"

import pandas as pd, io

df = pd.read_csv(data_path + "/stock_factors_data.csv")

def convertToLaTeX(df, alignment="c"):
    """
    Convert a pandas dataframe to a LaTeX tabular.
    Prints labels in bold, does not use math mode
    """
    numColumns = df.shape[1]
    numRows = df.shape[0]
    output = io.StringIO()
    colFormat = ("%s|%s" % (alignment, alignment * numColumns))
    #Write header
    output.write("$\\begin{document}\\begin{tabular}{%s}\n" % colFormat)
    columnLabels = ["\\textbf{%s}" % label for label in df.columns]
    output.write("& %s\\\\\\hline\n" % " & ".join(columnLabels))
    #Write data lines
    for i in range(numRows):
        output.write("\\textbf{%s} & %s\\\\\n"
                     % (df.index[i], " & ".join([str(val) for val in df.iloc[i]])))
    #Write footer
    output.write("\\end{tabular}\\end{document}$")
    return output.getvalue()

latex_test = df.head().style.to_latex()
latex_test = convertToLaTeX(df)
print(latex_test)

from pylatex import Document, Section, Subsection, Command
from pylatex.utils import italic, NoEscape

doc = Document()

doc.preamble.append(latex_test)

doc.generate_pdf('basic_maketitle', clean_tex=False)

