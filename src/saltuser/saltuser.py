import pandas as pd


class SALTUser:
    """
    A user of the Southern African Large Telescope with roles and permissions.

    The user is identified by their username, as used for the Principal Investigator
    Proposal Tool (PIPT) or the Web Manager.

    The aim of this class is to allow checking roles and permissions. It includes no
    authentication.

    You need to specify a database connection when creating the user. Any format allowed
    by the `con` parameter of pandas' `read_sql` function can be used.

    Parameters
    ----------
    username : str
        The username of the SALT user.
    db_engine : SQLAlchemy connectable(engine/connection) or database string URI
        A connection to the database to use, or its URI.

    Raises
    ------
    ValueError
        If there exists no user for the username.

    """

    def __init__(self, username, db_connectable):
        self._db_connectable = db_connectable
        self._user_id = self._find_user_id(username)
        self._tac_member_partners = self._find_tac_member_partners()
        self._tac_chair_partners = self._find_tac_chair_partners()

    def is_admin(self):
        """
        Check whether the user is an administrator.

        Returns
        -------
        bool
            Whether the user is an administrator.

        """

        sql = '''
SELECT Value
       FROM PiptUserSetting as pus
       JOIN PiptSetting ps on pus.PiptSetting_Id = ps.PiptSetting_Id
       JOIN PiptUser AS pu ON pus.PiptUser_Id = pu.PiptUser_Id
       WHERE pu.PiptUser_Id=%(user_id)s AND PiptSetting_Name='RightAdmin'
        '''
        df = self._query(sql, params=dict(user_id=self._user_id))

        return len(df) > 0 and int(df['Value'][0], 10) > 0

    def is_investigator(self, proposal_code):
        """
        Check whether the user is an investigator for a given proposal.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        bool
            Whether the user is an investigator for the proposal.

        """

        sql = '''
SELECT COUNT(*) AS User_Count
       FROM ProposalCode AS pc
       JOIN ProposalInvestigator pi on pc.ProposalCode_Id = pi.ProposalCode_Id
       JOIN Investigator AS i ON pi.Investigator_Id = i.Investigator_Id
       WHERE Proposal_Code=%(proposal_code)s AND PiptUser_Id=%(user_id)s
        '''
        df = self._query(sql, params=dict(proposal_code=proposal_code, user_id=self._user_id))

        return df['User_Count'][0] > 0

    def is_principal_investigator(self, proposal_code):
        """
        Check whether user is the Principal Investigator of a given proposal.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        bool
            Whether the user is the Principal Investigator of the proposal.

        """

        sql = '''
SELECT COUNT(*) AS User_Count
       FROM ProposalContact AS pco
       JOIN Investigator AS i ON pco.Leader_Id=i.Investigator_Id
       JOIN ProposalCode AS pc ON pco.ProposalCode_Id = pc.ProposalCode_Id
       WHERE Proposal_Code=%(proposal_code)s AND PiptUser_Id=%(user_id)s
        '''
        df = self._query(sql, params=dict(proposal_code=proposal_code, user_id=self._user_id))

        return df['User_Count'][0] > 0

    def is_principal_contact(self, proposal_code):
        """
        Check whether user is the Principal Contact of a given proposal.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        bool
            Whether the user is the Principal Contact of the proposal.

        """

        sql = '''
SELECT COUNT(*) AS User_Count
       FROM ProposalContact AS pco
       JOIN Investigator AS i ON pco.Contact_Id=i.Investigator_Id
       JOIN ProposalCode AS pc ON pco.ProposalCode_Id = pc.ProposalCode_Id
       WHERE Proposal_Code=%(proposal_code)s AND PiptUser_Id=%(user_id)s
        '''
        df = self._query(sql, params=dict(proposal_code=proposal_code, user_id=self._user_id))

        return df['User_Count'][0] > 0

    def is_tac_member(self, partner_code):
        """
        Check whether the user is member of a partner's TAC.

        Parameters
        ----------
        partner_code : str
            The partner code of the partner.

        Returns
        -------
        bool
           Whether the user is member of the partner's TAC.

        """
        return partner_code in self._tac_member_partners

    def is_proposal_tac_member(self, proposal_code):
        """
        Check whether the user is member of a TAC represented on a given proposal.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        bool
            Whether the user is member of a TAC represented on the proposal.

        """

        return len(set(self._tac_member_partners).intersection(self._proposal_partners(proposal_code))) > 0

    def is_tac_chair(self, partner_code):
        """
        Check whether the user is chair of a partner's TAC.

        Parameters
        ----------
        partner_code : str
            The partner code of the partner.

        Returns
        -------
        bool
           Whether the user is chair of the partner's TAC.

        """
        return partner_code in self._tac_chair_partners

    def may_view_proposal(self, proposal_code):
        """
        Check whether the user may view a given proposal.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        Whether the user mayu view the proposal.

        """

        return self.is_investigator(proposal_code) or self.is_proposal_tac_member(proposal_code) or self.is_admin()

    def may_edit_proposal(self, proposal_code):
        """
        Check whether the user may edit a given proposal.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        bool
            Whether the user may edit the proposal.

        """

        return self.is_principal_investigator(proposal_code) or self.is_principal_contact(proposal_code) or self.is_admin()

    def _query(self, sql, params):
        """
        Query the database.

        Depending on how they are referenced in the SQL query, the query parameters must
        be passed as an iterable or as a dict.

        Parameters
        ----------
        sql : str
            The SQL query.
        params : iterable or dict
            The query parameters.

        Returns
        -------
        DataFrame
            A pandas data frame with the query results.

        """

        return pd.read_sql(sql, con=self._db_connectable, params=params)

    def _find_user_id(self, username):
        """
        Find the user id corresponding to a username.

        Parameters
        ----------
        username : str
            The username.

        Returns
        -------
        int
            The user id.

        Raises
        ------
        ValueError
            If the username does not exist or is ambiguous.

        """

        sql = '''
SELECT PiptUser_Id FROM PiptUser WHERE Username=%(username)s
        '''
        df = self._query(sql, params=dict(username=username))

        # sanity checks
        if len(df) == 0:
            raise ValueError("Username does not exist: {username}".format(username=username))
        if len(df) > 1:
            raise ValueError("Username is ambiguous: {username}".format(username=username))

        return df['PiptUser_Id'][0].item()

    def _proposal_partners(self, proposal_code):
        """
        Find the partners who are represented among a proposal's investigators.

        Parameters
        ----------
        proposal_code : str
            The proposal code.

        Returns
        -------
        list of str
            The list of partner codes.

        """

        sql = '''
SELECT DISTINCT Partner_Code
       FROM Partner AS p
       JOIN Institute AS ins ON p.Partner_Id = ins.Partner_Id
       JOIN Investigator AS i ON ins.Institute_Id = i.Institute_Id
       JOIN ProposalInvestigator pi on i.Investigator_Id = pi.Investigator_Id
       JOIN ProposalCode AS pc ON pi.ProposalCode_Id = pc.ProposalCode_Id
       WHERE Proposal_Code=%(proposal_code)s
        '''
        df = self._query(sql, params=dict(proposal_code=proposal_code))

        return df['Partner_Code'].tolist()

    def _find_tac_member_partners(self):
        """
        Find the partners of whose TACs the user is a member.

        Returns
        -------
        list of str
            The partner codes.

        """

        sql = '''
SELECT Partner_Code
       FROM PiptUserTAC AS putac
       JOIN Partner AS p ON putac.Partner_Id = p.Partner_Id
       WHERE PiptUser_Id=%(user_id)s
        '''
        df = self._query(sql, params=dict(user_id=self._user_id))

        return [pc for pc in df['Partner_Code'].tolist()]

    def _find_tac_chair_partners(self):
        """
        Find the partners of whose TACs the user is chair.

        Returns
        -------
        list of str
            The partner codes.

        """

        sql = '''
SELECT Partner_Code
       FROM PiptUserTAC AS putac
       JOIN Partner AS p ON putac.Partner_Id = p.Partner_Id
       WHERE PiptUser_Id=%(user_id)s AND Chair=1
            '''
        df = self._query(sql, params=dict(user_id=self._user_id))

        return [pc for pc in df['Partner_Code'].tolist()]
