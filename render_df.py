"""
This file can be used to merge several downloaded data frames into one large data set. The user has to
indicate the correct path destination where the data subsets are stored. The output is then stored in
the same directory. The merging and processing takes a long time.

The function "merge" has the following arguments:
- start_date: The start date specified in the data_processing file.
- end_date: The start date specified in the data_processing file.
- path: The path where the user intends to store the data. The default is "".
- download: Whether the user wants to download the data or get them returned. The default is True.

The function "merge" returns a pd dataframe with columns for id, date, price, market_cap, and total_volume
"""

__all__ = ["render_summary_statistics"]

def render_summary_statistics(df):

    import io, os, subprocess, easydict, time

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

    time.sleep(3)
    # the path where the PDF is stored
    pdf_path = os.getcwd() + "/cover.pdf"

    # inverting the colors in the PDF in case the user is using dark mode
    if input("Is your editor is dark mode? y/n") in ["Y", "y"]:

        from pdf2image import convert_from_path
        from PIL import ImageChops

        pil_image_lst = convert_from_path(pdf_path)
        image = pil_image_lst[0]
        image = ImageChops.invert(image)
        image.save(pdf_path)
        time.sleep(3)