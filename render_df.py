"""
"""

__all__ = ["pdf_from_df"]

def pdf_from_df(df):

    import io, argparse, os, subprocess

    

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
        output.write("\\documentclass{article}\\begin{document}\\begin{tabular}{%s}\n" % colFormat)
        columnLabels = ["\\textbf{%s}" % label for label in df.columns]
        output.write("& %s\\\\\\hline\n" % " & ".join(columnLabels))
        #Write data lines
        for i in range(numRows):
            output.write("\\textbf{%s} & %s\\\\\n"
                        % (df.index[i], " & ".join([str(val) for val in df.iloc[i]])))
        #Write footer
        output.write("\\end{tabular}\\end{document}")
        return output.getvalue()

    content = convertToLaTeX(df)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--course')
    parser.add_argument('-t', '--title')
    parser.add_argument('-n', '--name',) 
    parser.add_argument('-s', '--school', default='My U')

    args = parser.parse_args()

    with open('cover.tex','w') as f:
        f.write(content%args.__dict__)

    cmd = ['pdflatex', '-interaction', 'nonstopmode', 'cover.tex']
    proc = subprocess.Popen(cmd)
    proc.communicate()

    retcode = proc.returncode
    if not retcode == 0:
        os.unlink('cover.pdf')
        raise ValueError('Error {} executing command: {}'.format(retcode, ' '.join(cmd))) 

    os.unlink('cover.tex')
    os.unlink('cover.log')
    os.unlink('cover.aux')

    from wand.image import Image as WImage
    img = WImage(filename=os.getcwd() + "cover.pdf", resolution=100)
    img



