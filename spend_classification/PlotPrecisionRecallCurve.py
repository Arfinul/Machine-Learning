import pandas as pd
from sklearn.metrics import average_precision_score
from sklearn import preprocessing
from sklearn.preprocessing import label_binarize
from sklearn.metrics import precision_recall_curve
import matplotlib.pyplot as plt
import os
import importlib.util
import numpy as np


# This class is written to draw precision recall curve
class PrecisionRecallCurve():

    def __init__(self, Y_test, y_score):
        self.Y_test = Y_test
        self.y_score = y_score

    def plot_curve(self):
        n_classes = self.Y_test.shape[1]

        precision = dict()
        recall = dict()
        average_precision = dict()
        threshold = dict()
        for i in range(n_classes):
            precision[i], recall[i], threshold[i] = precision_recall_curve(self.Y_test[:, i],
                                                                           self.y_score[:, i])
            average_precision[i] = average_precision_score(self.Y_test[:, i], self.y_score[:, i])

        # A "micro-average": quantifying score on all classes jointly
        precision["micro"], recall["micro"], _ = precision_recall_curve(self.Y_test.ravel(),
                                                                        self.y_score.ravel())
        average_precision["micro"] = average_precision_score(self.Y_test, self.y_score,
                                                             average="micro")
        print('Average precision score, micro-averaged over all classes: {0:0.2f}'
              .format(average_precision["micro"]))

        plt.figure()
        plt.step(recall['micro'], precision['micro'], color='b', alpha=0.2,
                 where='post')
        plt.fill_between(recall["micro"], precision["micro"], step='post', alpha=0.2,
                         color='b')

        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.ylim([0.0, 1.05])
        plt.xlim([0.0, 1.0])
        plt.title(
            'Average precision score, micro-averaged over all classes: AP={0:0.2f}'
                .format(average_precision["micro"]))

        # ------
        from itertools import cycle
        # setup plot details
        colors = cycle(['navy', 'turquoise', 'darkorange', 'cornflowerblue', 'teal'])

        plt.figure(figsize=(7, 8))
        f_scores = np.linspace(0.2, 0.8, num=4)
        lines = []
        labels = []
        for f_score in f_scores:
            x = np.linspace(0.01, 1)
            y = f_score * x / (2 * x - f_score)
            l, = plt.plot(x[y >= 0], y[y >= 0], color='gray', alpha=0.2)
            plt.annotate('f1={0:0.1f}'.format(f_score), xy=(0.9, y[45] + 0.02))

        lines.append(l)
        labels.append('iso-f1 curves')
        l, = plt.plot(recall["micro"], precision["micro"], color='gold', lw=2)
        lines.append(l)
        labels.append('micro-average Precision-recall (area = {0:0.2f})'
                      ''.format(average_precision["micro"]))

        for i, color in zip(range(n_classes), colors):
            l, = plt.plot(recall[i], precision[i], color=color, lw=2)
            lines.append(l)
            labels.append('Precision-recall for class {0} (area = {1:0.2f})'
                          ''.format(i, average_precision[i]))

        fig = plt.gcf()
        fig.subplots_adjust(bottom=0.25)
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.title('Extension of Precision-Recall curve to multi-class')
        plt.legend(lines, labels, loc=(0, -.38), prop=dict(size=14))

        plt.show()

