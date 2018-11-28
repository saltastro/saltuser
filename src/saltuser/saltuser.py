import pandas as pd


class SALTUser:
    """
    A user of the Southern African Large Telescope with roles and permissions.

    The user is identified by their username, as used for the Principal Investigator
    Proposal Tool (PIPT) or the Web Manager.

    The aim of this class is to allow checking roles and permissions. It includes no
    authentication.

    A new user should be created using either the constructor or the
    :meth:`find_by_username` method.

    You need to specify a database connection when creating the user. Any format allowed
    by the `con` parameter of pandas' `read_sql` function can be used.

    Parameters
    ----------
    user_id : int
        The user id.
    db_connectable : SQLAlchemy connectable(engine/connection) or database string URI
        A connection to the database to use, or its URI.

    Raises
    ------
    ValueError
        If the user does not exist.

    """

    def __init__(self, user_id, db_connectable):
        # sanity check: does the user exist?
        sql = """
SELECT pu.PiptUser_Id, FirstName, Surname, Email
       FROM PiptUser AS pu
       JOIN Investigator AS i USING (Investigator_Id)
       WHERE pu.PiptUser_Id=%(user_id)s
"""
        df = pd.read_sql(sql, con=db_connectable, params=dict(user_id=user_id))
        if len(df) == 0:
            raise ValueError(
                "There is no user with id {user_id}.".format(user_id=user_id)
            )

        self._db_connectable = db_connectable
        self._user_id = user_id
        self._given_name = df["FirstName"][0]
        self._family_name = df["Surname"][0]
        self._email = df["Email"][0]
        self._is_board_member = None
        self._tac_member_partners = self._find_tac_member_partners()
        self._tac_chair_partners = self._find_tac_chair_partners()
        self._viewable_proposals_cache = None

    @staticmethod
    def verify(username, password, db_connectable):
        """
        Verify that a username-password combination is valid.

        Parameters
        ----------
        username : str
            The username.
        password : str
            The password.
        db_connectable : SQLAlchemy connectable(engine/connection) or database string
                         URI
            A connection to the database to use, or its URI.

        Raises
        ------
        ValueError
            If the username or password are wrong.

        """

        sql = """
SELECT PiptUser_Id AS UserCount
       FROM PiptUser
       WHERE Username=%(username)s AND Password=MD5(%(password)s)
        """
        df = pd.read_sql(
            sql, con=db_connectable, params=dict(username=username, password=password)
        )
        if len(df) == 0:
            raise ValueError("invlid username or password")

    @staticmethod
    def find_by_username(username, db_connectable):
        """
        Get the user with a given username.

        Parameters
        ----------
        username : str
            The username.
        db_connectable : SQLAlchemy connectable(engine/connection) or database string
                         URI
            A connection to the database to use, or its URI.

        Returns
        -------
        SALTUser
            The SALT user.

        Raises
        ------
        ValueError
            If the user does not exist.

        """

        user_id = SALTUser._find_user_id(username, db_connectable)

        return SALTUser(user_id, db_connectable)

    @property
    def given_name(self):
        """
        Get the user's given name(s).

        Returns
        -------
        str
            The given name(s).

        """

        return self._given_name

    @property
    def family_name(self):
        """
        Get the user's family name.

        Returns
        -------
        str
            The family name.

        """

        return self._family_name

    @property
    def email(self):
        """
        Get the user's email address.

        Returns
        -------
        str
            The email address.

        """

        return self._email

    def is_admin(self):
        """
        Check whether the user is an administrator.

        Returns
        -------
        bool
            Whether the user is an administrator.

        """

        sql = """
SELECT Value
       FROM PiptUserSetting as pus
       JOIN PiptSetting ps on pus.PiptSetting_Id = ps.PiptSetting_Id
       JOIN PiptUser AS pu ON pus.PiptUser_Id = pu.PiptUser_Id
       WHERE pu.PiptUser_Id=%(user_id)s AND PiptSetting_Name='RightAdmin'
        """
        df = self._query(sql, params=dict(user_id=self._user_id))

        return len(df) > 0 and int(df["Value"][0], 10) > 0

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

        sql = """
SELECT COUNT(*) AS User_Count
       FROM ProposalCode AS pc
       JOIN ProposalInvestigator pi on pc.ProposalCode_Id = pi.ProposalCode_Id
       JOIN Investigator AS i ON pi.Investigator_Id = i.Investigator_Id
       WHERE Proposal_Code=%(proposal_code)s AND PiptUser_Id=%(user_id)s
        """
        df = self._query(
            sql, params=dict(proposal_code=proposal_code, user_id=self._user_id)
        )

        return df["User_Count"][0] > 0

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

        sql = """
SELECT COUNT(*) AS User_Count
       FROM ProposalContact AS pco
       JOIN Investigator AS i ON pco.Leader_Id=i.Investigator_Id
       JOIN ProposalCode AS pc ON pco.ProposalCode_Id = pc.ProposalCode_Id
       WHERE Proposal_Code=%(proposal_code)s AND PiptUser_Id=%(user_id)s
        """
        df = self._query(
            sql, params=dict(proposal_code=proposal_code, user_id=self._user_id)
        )

        return df["User_Count"][0] > 0

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

        sql = """
SELECT COUNT(*) AS User_Count
       FROM ProposalContact AS pco
       JOIN Investigator AS i ON pco.Contact_Id=i.Investigator_Id
       JOIN ProposalCode AS pc ON pco.ProposalCode_Id = pc.ProposalCode_Id
       WHERE Proposal_Code=%(proposal_code)s AND PiptUser_Id=%(user_id)s
        """
        df = self._query(
            sql, params=dict(proposal_code=proposal_code, user_id=self._user_id)
        )

        return df["User_Count"][0] > 0

    def is_board_member(self):
        """
        Check whether the user is a Board member.

        Returns
        -------
        bool
            Whether the user is a Board member.
        """

        if self._is_board_member is None:
            sql = """
SELECT *
       FROM PiptUserSetting
       WHERE PiptUser_Id=%(user_id)s
             AND PiptSetting_Id=
                     (SELECT PiptSetting_Id
                             FROM PiptSetting
                             WHERE PiptSetting_Name='RightBoard')
             AND Value>0
            """
            df = self._query(sql, dict(user_id=self._user_id))
            self._is_board_member = len(df) > 0

        return self._is_board_member

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

        return (
            len(
                set(self._tac_member_partners).intersection(
                    self._proposal_partners(proposal_code)
                )
            )
            > 0
        )

    @property
    def tacs(self):
        """
        The TACs (as a list of partner codes) on which the user serves.

        Returns
        -------
        list of str
            The partner codes of the TACs.
        """

        return self._tac_member_partners

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
        Whether the user may view the proposal.

        """

        return proposal_code in self._viewable_proposals

    @property
    def _viewable_proposals(self):
        """
        The proposals (as a list of proposal codes) the user may view.

        Returns
        -------
        list of str
            The list of proposal codes.
        """

        if self._viewable_proposals_cache is not None:
            return self._viewable_proposals_cache

        sql = """
SELECT DISTINCT Proposal_Code
       FROM ProposalCode AS pc
       JOIN ProposalInvestigator AS pi ON pc.ProposalCode_Id = pi.ProposalCode_Id
       JOIN Investigator AS i ON pi.Investigator_Id = i.Investigator_Id
       JOIN PiptUser AS pu ON i.PiptUser_Id=pu.PiptUser_Id
       JOIN Proposal AS p ON pc.ProposalCode_Id = p.ProposalCode_Id
       JOIN MultiPartner AS mp ON pc.ProposalCode_Id = mp.ProposalCode_Id
                                  AND p.Semester_Id = mp.Semester_Id
       JOIN Partner AS partner ON mp.Partner_Id = partner.Partner_Id
       WHERE pu.PiptUser_Id=%(user_id)s
             OR (partner.Partner_Code IN %(tacs)s AND mp.ReqTimeAmount>0)
             OR (1=%(is_admin)s)
             OR (1=%(is_board_member)s)
"""
        df = self._query(
            sql,
            params=dict(
                user_id=self._user_id,
                tacs=self.tacs if self.tacs else ["IMPOSSIBLE_VALUE"],
                is_admin=1 if self.is_admin() else 0,
                is_board_member=1 if self.is_board_member() else 0,
            ),
        )

        self._viewable_proposals_cache = set(df["Proposal_Code"].tolist())
        return self._viewable_proposals_cache

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

        return (
            self.is_principal_investigator(proposal_code)
            or self.is_principal_contact(proposal_code)
            or self.is_admin()
        )

    def may_view_block(self, block_id):
        """
        Check whether the user may view a given block.

        Parameters
        ----------
        block_id : int
            The block id.

        Returns
        -------
        bool
            Whether the user may view the block.

        Raises
        ------
        ValueError
            If there exists no block with the given block id.

        """

        proposal_code = self._proposal_code_of_block(block_id=block_id)

        return self.may_view_proposal(proposal_code=proposal_code)

    def may_edit_block(self, block_id):
        """
        Check whether the user may edit a given block.

        Parameters
        ----------
        block_id : int
            The block id.

        Returns
        -------
        bool
            Whether the user may edit the block.

        Raises
        ------
        ValueError
            If there exists no block with the given block id.

        """

        proposal_code = self._proposal_code_of_block(block_id=block_id)

        return self.may_edit_proposal(proposal_code=proposal_code)

    def _proposal_code_of_block(self, block_id):
        """
        Get the proposal code of the proposal containing a given block.

        Parameters
        ----------
        block_id : int
            The block id.

        Returns
        -------
        str
            The proposal code.

        Raises
        ------
        ValueError
            If there exists no block with the given block id.

        """

        sql = """
SELECT Proposal_Code
       FROM ProposalCode AS pc
       JOIN Block AS b ON pc.ProposalCode_Id = b.ProposalCode_Id
       WHERE Block_Id=%(block_id)s
        """
        df = self._query(sql, params=dict(block_id=block_id))

        # sanity check: does the block exist?
        if len(df) == 0:
            raise ValueError(
                "There exists no block with id {block_id}".format(block_id=block_id)
            )

        return df["Proposal_Code"][0]

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

    @staticmethod
    def _find_user_id(username, db_connectable):
        """
        Find the user id corresponding to a username.

        Parameters
        ----------
        username : str
            The username.
        db_connectable : SQLAlchemy connectable(engine/connection) or database string
                         URI
            A connection to the database to use, or its URI.

        Returns
        -------
        int
            The user id.

        Raises
        ------
        ValueError
            If the username does not exist.

        """

        sql = """
SELECT PiptUser_Id FROM PiptUser WHERE Username=%(username)s
        """
        df = pd.read_sql(sql, con=db_connectable, params=dict(username=username))

        # sanity check: does the user exist?
        if len(df) == 0:
            raise ValueError(
                "The username does not exist: {username}".format(username=username)
            )

        return df["PiptUser_Id"][0].item()

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

        sql = """
SELECT DISTINCT Partner_Code
       FROM Partner AS p
       JOIN Institute AS ins ON p.Partner_Id = ins.Partner_Id
       JOIN Investigator AS i ON ins.Institute_Id = i.Institute_Id
       JOIN ProposalInvestigator pi on i.Investigator_Id = pi.Investigator_Id
       JOIN ProposalCode AS pc ON pi.ProposalCode_Id = pc.ProposalCode_Id
       WHERE Proposal_Code=%(proposal_code)s
        """
        df = self._query(sql, params=dict(proposal_code=proposal_code))

        return df["Partner_Code"].tolist()

    def _find_tac_member_partners(self):
        """
        Find the partners of whose TACs the user is a member.

        Returns
        -------
        list of str
            The partner codes.

        """

        sql = """
SELECT Partner_Code
       FROM PiptUserTAC AS putac
       JOIN Partner AS p ON putac.Partner_Id = p.Partner_Id
       WHERE PiptUser_Id=%(user_id)s
        """
        df = self._query(sql, params=dict(user_id=self._user_id))

        return [pc for pc in df["Partner_Code"].tolist()]

    def _find_tac_chair_partners(self):
        """
        Find the partners of whose TACs the user is chair.

        Returns
        -------
        list of str
            The partner codes.

        """

        sql = """
SELECT Partner_Code
       FROM PiptUserTAC AS putac
       JOIN Partner AS p ON putac.Partner_Id = p.Partner_Id
       WHERE PiptUser_Id=%(user_id)s AND Chair=1
            """
        df = self._query(sql, params=dict(user_id=self._user_id))

        return [pc for pc in df["Partner_Code"].tolist()]
