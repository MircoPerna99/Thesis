/*Analysis network*/

/*Count PPI*/
/*767807*/
SELECT Count(*)
FROM PPI p 

/*Count DTI*/
/*25710*/
SELECT Count(*)
FROM DTI d

/*PPIsForAnalysis*/
CREATE VIEW PPIsForAnalysis AS
SELECT p.ProteinAId, p.ProteinBId, MAX(p.Score) AS Score
FROM PPI p
WHERE p.Score >= 0.5
GROUP BY p.ProteinAId, p.ProteinBId

/*Define AmountIntercation manually*/
CREATE VIEW AmountIntercation_Manually AS
SELECT d.DrugId, p.ProteinBId, COUNT(*) as 'AmountInteractions'
FROM DTI d 
INNER JOIN PPIsForAnalysis p ON d.ProteinId = p.ProteinAId AND p.Score >= 0.5
GROUP BY d.DrugId, p.ProteinBId

/*Count AmountIntercation*/
/*4308*/
SELECT Count(*)
FROM AmountInteraction ai

/*250638*/
SELECT COUNT(*)
FROM amountintercation_manually am 

/*3567*/
SELECT Count(*)
FROM AmountInteraction ai
INNER JOIN amountintercation_manually am  
			ON am.AmountInteractions = ai.amount_interactions
			and ai.drugId = am.DrugId  
			and ai.proteinId = am.ProteinBId
			

/*Define PPIsForProtein*/	
CREATE VIEW PPIsForProtein AS
SELECT p.ProteinAId as ProteinId , count(*) as AmountInteractionsProteins
FROM PPIsForAnalysis p 
GROUP BY p.ProteinAId

/*Degree distribution*/
SELECT  p.AmountInteractionsProteins, count(*)
FROM ppisforprotein p 
GROUP BY p.AmountInteractionsProteins
ORDER BY p.AmountInteractionsProteins

/*Define ProteinInteractionsForDrug*/
CREATE VIEW ProteinInteractionsForDrug AS
SELECT d.DrugId, COUNT(*) AS AmountInteractionsProteins
FROM DTI d 
GROUP BY d.DrugId
order by COUNT(*) desc

/*Degree distribution*/
SELECT p.AmountInteractionsProteins, count(*)
FROM proteininteractionsfordrug p 
GROUP BY p.AmountInteractionsProteins
ORDER BY p.AmountInteractionsProteins


/*Define DrugInteractionsForProtein*/
CREATE VIEW DrugInteractionsForProtein AS
SELECT d.ProteinId , COUNT(*) AS AmountInteractionsDrugs
FROM DTI d 
GROUP BY d.ProteinId
order by COUNT(*) desc

SELECT p.AmountInteractionsDrugs, count(*)
FROM DrugInteractionsForProtein p 
GROUP BY p.AmountInteractionsDrugs
ORDER BY p.AmountInteractionsDrugs



/*Complete*/
SELECT d.DrugId, p.ProteinBId, pd.AmountInteractionsProteins as 'AmountInteractionsProteinsDrug' ,p2.AmountInteractionsProteins, IFNULL(d2.AmountInteractionsDrugs, 0) AS 'AmountInteractionsDrugs' ,COUNT(*) as 'AmountInteractions'
FROM DTI d 
INNER JOIN PPIsForAnalysis p ON d.ProteinId = p.ProteinAId AND p.Score >= 0.5
INNER JOIN ppisforprotein p2 ON p2.ProteinId  = p.ProteinBId
INNER JOIN proteininteractionsfordrug pd ON pd.DrugId  = d.DrugId
LEFT JOIN druginteractionsforprotein d2 ON d2.ProteinId = p.ProteinBId
GROUP BY d.DrugId, p.ProteinBId, p2.AmountInteractionsProteins, pd.AmountInteractionsProteins




