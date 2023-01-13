"""
"""

__all__ = ["pdf_from_df"]

def pdf_from_df(df):

    import io, os, subprocess, easydict

    def convertToLaTeX(df, alignment="c"):
        # this function converts a df dataframe to a LaTeX tabular
        # it prints labels in bold and does not use math mode
        
        numColumns = df.shape[1]
        numRows = df.shape[0]
        output = io.StringIO()
        colFormat = ("%s|%s" % (alignment, alignment * numColumns))
        #Write header
        output.write("\\documentclass{article}\\begin{document}\\begin{tabular}{%s}\n" % colFormat)
        columnLabels = ["\\textbf{%s}" % label for label in df.columns]
        output.write("& %s\\\\\\hline\n" % " & ".join(columnLabels))
        #Write data lines
        for i in range(numRows):
            output.write("\\textbf{%s} & %s\\\\\n"
                        % (df.index[i], " & ".join([str(val) for val in df.iloc[i]])))
        #Write footer
        output.write("\\pagenumbering{gobble}\\end{tabular}\\end{document}")
        return output.getvalue()

    content = convertToLaTeX(df)

    args = easydict.EasyDict({})

    with open('cover.tex','w') as f:
        f.write(content%args.__dict__)

    cmd = ['pdflatex', '-interaction', 'nonstopmode', 'cover.tex']
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink('cover.pdf')
        raise ValueError('Error {} executing command: {}'.format(retcode, ' '.join(cmd))) 

    os.unlink('cover.tex')
    os.unlink('cover.log')
    os.unlink('cover.aux')